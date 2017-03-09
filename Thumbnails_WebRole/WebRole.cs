using Microsoft.WindowsAzure.ServiceRuntime;
using System.Net;
using Microsoft.WindowsAzure.Storage;
using System.Diagnostics;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Thumbnails_WebRole
{
    public class WebRole : RoleEntryPoint
    {
        private CloudQueue videosQueue;
        private CloudBlobContainer videoBlobContainer;

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections.
            ServicePointManager.DefaultConnectionLimit = 12;

            // Obtain connection to this application's storage account 
            // using credentials set in role properties and embedded in .cscfg file.
            // It will be used to access both blobs and queues. 
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse
                (RoleEnvironment.GetConfigurationSettingValue("StorageConnectionString"));

            // Set up blob container, creating if necessary

            // Instantiate the logical client object used for
            // communicating with a blob container.  
            Trace.TraceInformation("Creating videos blob container");
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Associate that logical client object with a physical
            // blob container. If we knew that the blob container
            // already existed, this would be all that we needed.            
            videoBlobContainer = blobClient.GetContainerReference("videos");

            // Create the physical blob container underlying the logical
            // CloudBlobContainer object, if it doesn't already exist. A 
            // production app will frequently not do this, instead
            // requiring the initial administrative provisioning 
            // process to set up blob containers and other storage structures. 
            if (videoBlobContainer.CreateIfNotExists())
            {
                // Enable public access on the newly created "photogallery" container.
                // i.e. set the permission on the blob container to allow anonymous access.
                videoBlobContainer.SetPermissions(
                    new BlobContainerPermissions
                    {
                        PublicAccess = BlobContainerPublicAccessType.Blob
                    });
            }

            // Set up queue, creating if necessary

            // Instantiate a client object for communicating
            // with a message queue
            Trace.TraceInformation("Creating thumbnails queue");
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();

            // Connect the client object to a specific CloudQueue
            // logical object in the storage service. If we were
            // sure that physical queue underlying this logical 
            // object already existed, this would be all we needed.
            videosQueue = queueClient.GetQueueReference("videoqueue");

            // Create the physical queue underlying the logical
            // CloudQueue object, if it doesn't already exist. A 
            // production app will frequently not do this, instead
            // requiring the initial administrative provisioning 
            // process to set up queues and other storage structures. 
            videosQueue.CreateIfNotExists();

            Trace.TraceInformation("Storage initialized");
            Trace.TraceInformation("Thumbnails_WebRole is running");

            return base.OnStart();
        }
    }
}
