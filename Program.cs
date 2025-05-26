using Azure.Identity;
using Azure.Messaging.ServiceBus;
using IndexingWorkerContainerApp;
using IndexingWorkerContainerApp.Services;
using IndexingWorkerContainerApp.ViewModels;
using Microsoft.ApplicationInsights.WorkerService;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;
using VerataShared.BlobStorage;
using VerataShared.TokenAuth;
using VerataShared.VerataEnvironment;
using Secrets = IndexingWorkerContainerApp.ViewModels.Secrets;

var host = new HostBuilder()
    .ConfigureFunctionsWebApplication()
    .ConfigureAppConfiguration((hostingContext, config) =>
    {
       var configBuilder = config.SetBasePath( Directory.GetCurrentDirectory() )
             .AddJsonFile(  "functionappsettings.json",
               optional: false,
               reloadOnChange: true)
             .AddJsonFile($"functionappsettings.{hostingContext.HostingEnvironment.EnvironmentName}.json",
               optional: true,
               reloadOnChange: false)
            .AddEnvironmentVariables()
            .Build();

       var environmentName = hostingContext.HostingEnvironment.EnvironmentName;

       var vaultUrl = configBuilder.GetSection($"VaultURL:{environmentName}").Value;
       if( !string.IsNullOrEmpty(vaultUrl) )
       {
          config.AddAzureKeyVault(new Uri(vaultUrl), new DefaultAzureCredential());
       }

       if ( environmentName.Equals( "DEVELOPMENT", StringComparison.OrdinalIgnoreCase ) )
       {
          // for local debugging, User Secrets override other settings
          config.AddUserSecrets( System.Reflection.Assembly.GetExecutingAssembly(), true );
       }
    })
    .ConfigureServices(services =>
    {
       var configuration = services.BuildServiceProvider().GetService<IConfiguration>();
       if( configuration is null )
       {
          throw new ArgumentNullException( "Unable to retrieve configuration", nameof(configuration) );
       }
       var environmentName = configuration.GetSection("ASPNETCORE_ENVIRONMENT").Value;
       var hostingEnvironment = new EnvironmentService( environmentName, configuration );

       services.TryAddSingleton<IVerataEnvironmentVariable>( hostingEnvironment );

       var secrets = configuration.Get<Secrets>();
       if( secrets is null )
       {
          throw new ArgumentNullException( "Unable to retrieve secrets", nameof(secrets) );
       }

       services.TryAddSingleton( secrets );

       var indexingWorkerServiceBusCredsJson = configuration[nameof( Secrets.IndexingWorkerServiceBusEntityCredentials )];
       if ( !string.IsNullOrEmpty( indexingWorkerServiceBusCredsJson ) )
       {
          var serviceBusEntityCredentials = JsonConvert.DeserializeObject<ServiceBusEntityCredentials>(
             indexingWorkerServiceBusCredsJson );
          if( serviceBusEntityCredentials is null )
          {
             throw new ArgumentNullException( "Unable to indexing worker service bus creds",
                nameof(Secrets.IndexingWorkerServiceBusEntityCredentials) );
          }

          secrets.IndexingWorkerServiceBusEntityCredentials = serviceBusEntityCredentials;
       }

       services.AddTransient<AppInsightsTelemetryInitializer>();

       var aiOptions = new ApplicationInsightsServiceOptions();
       configuration.GetSection( "ApplicationInsights" ).Bind( aiOptions );
       aiOptions.ConnectionString = secrets.ApplicationInsights.ConnectionString;

       services.AddApplicationInsightsTelemetryWorkerService( aiOptions );
       services.ConfigureFunctionsApplicationInsights();

       services.AddScoped<IDictionary<BlobStorageServiceName, IBlobStorage>>( factory =>
         {
            var ocrBlobStorage =  new AzureBlobStorage( new AzureBlobSettings(
               storageAccount: secrets.OcrBlob.StorageAccount,
               storageKey: secrets.OcrBlob.StorageKey,
               containerName: secrets.OcrBlob.ContainerName ) );

            var tokenAuthBlobStorage =  new AzureBlobStorage( new AzureBlobSettings(
               storageAccount: secrets.Blob.StorageAccount,
               storageKey: secrets.Blob.StorageKey,
               containerName: secrets.Blob.ContainerName ) );

            return new Dictionary<BlobStorageServiceName, IBlobStorage>
            {
                { BlobStorageServiceName.Ocr, ocrBlobStorage },
                { BlobStorageServiceName.TokenAuth, tokenAuthBlobStorage }
            };
         } );

       services.AddAzureClients(clientBuilder =>
       {
          clientBuilder.AddServiceBusClient(secrets.IndexingWorkerServiceBusEntityCredentials.ConnectionString)
               .WithName(secrets.IndexingWorkerServiceBusEntityCredentials.EntityName)
               .ConfigureOptions(options =>
                                  {
                                     options.RetryOptions.Mode = ServiceBusRetryMode.Exponential;
                                     options.RetryOptions.MaxRetries = 3;
                                     options.RetryOptions.MaxDelay = TimeSpan.FromSeconds( 5 );
                                  } );
       });

       services.AddIndexingWorkerServiceExtensions();
    })
    .Build();

host.Run();