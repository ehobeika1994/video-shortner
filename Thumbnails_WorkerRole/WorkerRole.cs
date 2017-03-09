using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using System;

namespace Thumbnails_WorkerRole
{
    public class WorkerRole : RoleEntryPoint
    {
        private CloudQueue videosQueue;
        private CloudBlobContainer videosBlobContainer;

        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        private String fullInPath;
        private String fullOutPath;
        private String fileTitle;
        Stopwatch stopWatch = new Stopwatch();

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections.
            ServicePointManager.DefaultConnectionLimit = 12;

            // Open storage account using credentials set in role properties and embedded in .cscfg file.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse
                (RoleEnvironment.GetConfigurationSettingValue("StorageConnectionString"));

            Trace.TraceInformation("Creating videogallery blob container");
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            videosBlobContainer = blobClient.GetContainerReference("videos");
            if (videosBlobContainer.CreateIfNotExists())
            {
                // Enable public access on the newly created "videogallery" container.
                videosBlobContainer.SetPermissions(
                    new BlobContainerPermissions
                    {
                        PublicAccess = BlobContainerPublicAccessType.Blob
                    });
            }

            Trace.TraceInformation("Creating video crop queue");
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            videosQueue = queueClient.GetQueueReference("videoqueue");
            videosQueue.CreateIfNotExists();

            Trace.TraceInformation("Storage initialized");
            return base.OnStart();
        }

        public override void Run()
        {
            Trace.TraceInformation("Thumbnails_WorkerRole is running");

            CloudQueueMessage msg = null;

            while (true)
            {
                // 30s wait for demo - look in message queue - comment out as appropriate
                //Thread.Sleep(30000);

                try
                {
                    msg = this.videosQueue.GetMessage();
                    if (msg != null)
                    {
                        ProcessQueueMessage(msg);
                    }
                    else
                    {
                        Thread.Sleep(1000);
                    }
                }
                catch (StorageException e)
                {
                    if (msg != null && msg.DequeueCount > 5)
                    {
                        this.videosQueue.DeleteMessage(msg);
                        Trace.TraceError("Deleting poison queue item: '{0}'", msg.AsString);
                    }
                    Trace.TraceError("Exception in Thumbnails_WorkerRole: '{0}'", e.Message);
                    Thread.Sleep(5000);
                }
            }
        }

        /*private void ProcessQueueMessage(CloudQueueMessage msg)
        {
            // The message's contents contains the name of 
            // the blob containing the complete photo. 
            string path = msg.AsString;
            Trace.TraceInformation(string.Format("*** WorkerRole: Dequeued '{0}'", path));

            // Fetch the blob containing the video
            CloudBlockBlob inputBlob = videosBlobContainer.GetBlockBlobReference(path);

            // Create a new blob with the string "video/" prepended
            // to the video name. Set its ContentType property.
            string videoName = Path.GetFileNameWithoutExtension(inputBlob.Name) + "video.mp4";
            CloudBlockBlob outputBlob = this.videosBlobContainer.GetBlockBlobReference("videos/" + videoName);
            Log("new file name: " + videoName);

            //Notice that UploadVideo() can read/write directly to the blobs using streams
            using (Stream input = inputBlob.OpenRead())
            using (Stream output = outputBlob.OpenWrite())

            {
                ConvertSampleVideo(input, output);
                outputBlob.Properties.ContentType = "video/mp4";
            }


            Trace.TraceError("Generated video in blob {0}", videoName);

            // Delete the message from the queue. This isn't
            // done until all the processing has been successfully
            // accomplished, so we know we won't miss one in the case of 
            // an exception. However, in that case, we might execute
            // this code more than once. This is a good example
            // of "at least once" design, which is appropriate 
            // for many cases. 
            videosQueue.DeleteMessage(msg);
        }*/

        private void ProcessQueueMessage(CloudQueueMessage msg)
        {
            //get file's path from message queue
            string path = msg.AsString;

            //get input blob
            CloudBlockBlob inputBlob = videosBlobContainer.GetBlockBlobReference(path);

            //make folder for blob to be downloaded into
            string folder = path.Split('\\')[0];
            System.IO.Directory.CreateDirectory(GetLocalStoragePath() + @"\" + folder);

            //download file to local storage
            Log("Downloading blob to local storage...");
            videosBlobContainer.GetBlockBlobReference(path).DownloadToFile(GetLocalStoragePath() + path, FileMode.Create);
            Log("Done downloading");

            //get file's current location
            fullInPath = GetLocalStoragePath() + path;

            //new file name
            string videoName = Path.GetFileNameWithoutExtension(inputBlob.Name) + "-video-cropped.mp4";
            Log("New file name: " + videoName);

            //get and make directory for file output
            fullOutPath = GetLocalStoragePath() + @"out\" + videoName;
            CloudBlockBlob outputBlob = this.videosBlobContainer.GetBlockBlobReference(@"out\" + videosBlobContainer);
            Directory.CreateDirectory(GetLocalStoragePath() + @"out\");

            //shorten the video to 10s
            Log("Shortening MP4 to 10s.");
            stopWatch.Start();

            ConvertSampleVideo(10);

            stopWatch.Stop();
            Log("It took about " + stopWatch.ElapsedMilliseconds + " ms to shorten the mp4 video file.");
            stopWatch.Reset();

            //set content type to mp4
            outputBlob.Properties.ContentType = "video/mp4";

            //set tags
            Log("Tags.");
            TagLib.File tagFile = TagLib.File.Create(fullOutPath);

            tagFile.Tag.Comment = "Shortened on WorkerRole Instance " + GetInstanceIndex();
            tagFile.Tag.Conductor = "Edmond Hobeika - S1238520";
            //Check if the title tag is null
            fileTitle = tagFile.Tag.Title ?? "File has no original Title Tag";
            tagFile.Save();

            LogMP4Metadata(tagFile);

            //upload blob  from local storage to container
            Log("Returning mp4 to the blob container.");
            using (var fileStream = File.OpenRead(fullOutPath))
            {
                outputBlob.UploadFromStream(fileStream);
            }

            //Add metadata to blob
            Log("Adding metadata to the blob.");
            outputBlob.FetchAttributes();
            outputBlob.Metadata["Title"] = fileTitle;
            outputBlob.Metadata["InstanceNo"] = GetInstanceIndex();
            outputBlob.SetMetadata();

            //Print blob metadata to console
            Log("Blob's metadata: ");
            foreach (var item in outputBlob.Metadata)
            {
                Log("   " + item.Key + ": " + item.Value);
            }

            //remove message from queue
            Log("Removing message from the queue.");
            videosQueue.DeleteMessage(msg);

            //remove initial blob
            Log("Deleting the input blob.");
            inputBlob.Delete();

            //remove files from local storage
            Log("Deleting files from local storage.");
            File.Delete(fullInPath);
            File.Delete(fullOutPath);
        }

        public static String GetExePath()
        {
            return Path.Combine(Environment.GetEnvironmentVariable("RoleRoot") + @"\", @"approot\ffmpeg.exe");
        }

        public static String GetExeArgs(String inPath, String outPath, int seconds = 10)
        {
            return "-t " + seconds + " -i " + inPath + " -map_metadata 0 -acodec copy " + outPath + " -y";
        }

        public static String GetLocalStoragePath()
        {
            // return the full path of the local storage
            LocalResource lr = RoleEnvironment.GetLocalResource("LocalVideoStore");
            return string.Format(lr.RootPath);
        }

        public static String GetInstanceIndex()
        {
            // returns the instace's index
            string instanceId = RoleEnvironment.CurrentRoleInstance.Id;
            return instanceId.Substring(instanceId.LastIndexOf("_") + 1);
        }

        // Create cropped video - the detail is unimportant
        public bool ConvertSampleVideo(int seconds = 10)
        {
            bool success = false;

            /* using (Stream fileStream = File.Create(GetLocalStoragePath() + "input.mp4"))
             {
                 fileStream.Seek(0, SeekOrigin.Begin);
                 fileStream.CopyTo(input);
             }*/

            try
            {
                Log(GetExePath());
                Log(GetExeArgs(fullInPath, fullOutPath, seconds));

                Process proc = new Process();
                //set exe's location
                proc.StartInfo.FileName = GetExePath();
                // set command line args
                proc.StartInfo.Arguments = GetExeArgs(fullInPath, fullOutPath, seconds);
                proc.StartInfo.CreateNoWindow = true;
                proc.StartInfo.UseShellExecute = false;
                proc.StartInfo.ErrorDialog = false;

                // execute code
                proc.Start();
                proc.WaitForExit();
                success = true;

                Log("It worked!");
            }
            catch (Exception e)
            {
                Trace.TraceError(e.StackTrace);
            }

            /* using (Stream fileStream = File.OpenRead(GetLocalStoragePath() + "output.mp4"))
              {
                  fileStream.Seek(0, SeekOrigin.Begin);
                  fileStream.CopyTo(output);
              }*/

            return success;

        }



        protected void LogMP4Metadata(TagLib.File file)
        {
            Log("File's metadata:");

            Log("Title: " + file.Tag.Title);

        }

        //Short-hand method to write to Azure Compute Emulator's console.
        protected void Log(String msg)
        {
            Trace.TraceInformation(msg);
        }

        public override void OnStop()
        {
            Trace.TraceInformation("Thumbnails_WorkerRole is stopping");

            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();

            base.OnStop();

            Trace.TraceInformation("Thumbnails_WorkerRole has stopped");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following with your own logic.
            while (!cancellationToken.IsCancellationRequested)
            {
                Trace.TraceInformation("Working");
                await Task.Delay(1000);
            }
        }
    }
}
