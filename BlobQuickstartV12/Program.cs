using System;
using System.IO;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Storage.Blobs;

namespace BlobQuickstartV12
{
  class Program
  {
    static async Task Main()
    {
      string connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING");
      // Create a BlobServiceClient object which will be used to create a container client
      BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString);
      //Create a unique name for the container
      string containerName = "sbdia";
      // Create the container and return a container client object
      BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
      // Create a local file in the ./data/ directory for uploading and downloading
      string localPath = "./data/";
      string fileName = Guid.NewGuid().ToString() + ".txt";
      string localFilePath = Path.Combine(localPath, fileName);
      // Write text to the file
      await File.WriteAllTextAsync(localFilePath, "Hello, World!");
      // Get a reference to a blob
      BlobClient blobClient = containerClient.GetBlobClient(fileName);
      Console.WriteLine("Uploading to Blob storage as blob:\n\t {0}\n", blobClient.Uri);
      // Open the file and upload its data
      using FileStream uploadFileStream = File.OpenRead(localFilePath);
      await blobClient.UploadAsync(uploadFileStream, true);
      uploadFileStream.Close();
      await ReceiveMessagesAsync();
    }
    static async Task ReceiveMessagesAsync()
    {
      string connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING_SERVICE_BUS");
      string queueName = "sbdia";
      await using (ServiceBusClient client = new ServiceBusClient(connectionString))
      {
        // create a processor that we can use to process the messages
        ServiceBusProcessor processor = client.CreateProcessor(queueName, new ServiceBusProcessorOptions());

        // add handler to process messages
        processor.ProcessMessageAsync += MessageHandler;

        // add handler to process any errors
        processor.ProcessErrorAsync += ErrorHandler;

        // start processing 
        await processor.StartProcessingAsync();

        Console.WriteLine("Wait for a minute and then press any key to end the processing");
        Console.ReadKey();

        // stop processing 
        Console.WriteLine("\nStopping the receiver...");
        await processor.StopProcessingAsync();
        Console.WriteLine("Stopped receiving messages");
      }
    }
    static async Task MessageHandler(ProcessMessageEventArgs args)
    {
      string body = args.Message.Body.ToString();
      Console.WriteLine($"Received: {body}");

      // complete the message. messages is deleted from the queue. 
      await args.CompleteMessageAsync(args.Message);
    }

    // handle any errors when receiving messages
    static Task ErrorHandler(ProcessErrorEventArgs args)
    {
      Console.WriteLine(args.Exception.ToString());
      return Task.CompletedTask;
    }

  }
}
