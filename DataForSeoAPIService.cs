using dataconnectors_library.Interfaces;
using dataconnectors_library.Models;
using Microsoft.VisualBasic;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Newtonsoft.Json;
using Azure;
using System.Runtime.InteropServices;

namespace dataconnectors_library.Managers
{
    public class DataForSeoAPIService : IDataForSeoManager
    {
        // Fields to store service dependencies and connector name
        private string connectorname = "dataforseo";
        private IKeywordTableStorageService _keywordTableStorageService;
        private IDataForSeoService _dataForSeoService;
        private IDataLakeService _dataLakeService;
        private IDataFactoryService _dataFactoryService;
        private readonly IKeywordSqlService _sqlService;
        private readonly IDomainSqlService _domainSqlService;
        private readonly IDomainOnDemandSqlService _domainOnDemandSqlService;

        // Constructor to inject dependencies
        public DataForSeoAPIService(
            IKeywordTableStorageService keywordTableStorageService,
            IDataForSeoService dataForSeoService,
            IDataLakeService dataLakeService,
            IDataFactoryService dataFactoryService,
            IKeywordSqlService keywordSqlService,
            IDomainSqlService domainSqlService,
            IDomainOnDemandSqlService domainOnDemandSqlService)
        {
            _keywordTableStorageService = keywordTableStorageService;
            _dataForSeoService = dataForSeoService;
            _dataLakeService = dataLakeService;
            _sqlService = keywordSqlService;
            _dataFactoryService = dataFactoryService;
            _domainSqlService = domainSqlService;
            _domainOnDemandSqlService = domainOnDemandSqlService;
        }

        // Method to get SERP data
        public async Task GetSerpData(DataForSeoSerpPostModel model)
        {
            try
            {
                // Read Keywords from database
                var lKeywords = await _sqlService.GetKeywords("Keywords50000", "dbo");
                var calltype = "serp_task_post";
                // Group tasks in batches of 100

                bool continuePaging = true;
                int pagesize = 100;
                var page = 0;

                // Pauze for 1min after x amount of calls
                int pauzeAfterCalls = 1000;
                int dataForSeoPauzeTimeInMinutes = 1;

                Guid runid = Guid.NewGuid();

                // Door de minuut wachten en de skip take, kunnen we geen parallel foreach meer doen.
                while (continuePaging)
                {
                    var lNext100Keywords = lKeywords.Skip(page * pagesize).Take(pagesize);
                    if (lNext100Keywords.Count() < pagesize)
                        continuePaging = false;

                    if (lNext100Keywords.Count() > 0)
                    {
                        var responseJson = await _dataForSeoService.GetTop100RefferersQueue(runid, lNext100Keywords, connectorname, calltype);
                    }
                    page++;

                    if (page % pauzeAfterCalls == 0)
                    {
                        Thread.Sleep(new TimeSpan(0, dataForSeoPauzeTimeInMinutes, 0));
                    }
                }

                WebhookReturnPostObject postObject = new($"Successfully queue'd {lKeywords.Count} keywords to DataForSeo", string.Empty, StatusCodeEnum.Ok);
                await _dataFactoryService.CallWebhook(postObject, model.CallBackUri);
            }
            catch (Exception ex)
            {
                WebhookReturnPostObject postObject = new("Failed to queue the keywords to DataForSeo, see exception for more details", ex.ToString(), StatusCodeEnum.BadRequest);
                await _dataFactoryService.CallWebhook(postObject, model.CallBackUri);
            }
        }

        public async Task GetBacklinks(DataForSeoBacklinksPostModel model)
        {
            try
            {
                // Read domains for dim.domain (via ondemand database)
                var lDomains = await _domainSqlService.GetFullTable("Domains", "dbo", new string[] { "domain" });

                var calltype = "backlinks_live";

                // Pauze for 1min after x amount of calls
                int pauzeAfterCalls = 2000;
                int pauzeTimeInMinutes = 1;

                Guid runid = Guid.NewGuid();

                List<string> lResponseJson = new List<string>();

                for (int i = 1; i <= lDomains.Count; i++)
                {
                    var domain = lDomains[i - 1];
                    // Er lijken alleen Live calls te zijn, voer dus de live calls uit

                    bool continuePaging = true;
                    string nextToken = null;
                    int currentCount = 0;

                    while (continuePaging)
                    {

                        var response = await _dataForSeoService.BacklinksLive(domain[0], model.Mode, model.IncludeSubdomains, model.StatysType, currentCount, nextToken);
                        lResponseJson.Add(response.Result);

                        continuePaging = response.Continue;
                        nextToken = response.NextToken;
                        currentCount = response.Count;

                    }

                    await SaveJsonsToTableStorage(calltype, runid, lResponseJson);
                    lResponseJson.Clear();

                    if (i % pauzeAfterCalls == 0)
                    {
                        Thread.Sleep(new TimeSpan(0, pauzeTimeInMinutes, 0));
                    }
                }

                if (lResponseJson.Count > 0)
                {
                    await SaveJsonsToTableStorage(calltype, runid, lResponseJson);
                }

                await SaveCallSettingsToTableStorage(calltype, runid, model);

                WebhookReturnPostObject postObject = new($"Successfully live called {lDomains.Count} domains to DataForSeo", string.Empty, StatusCodeEnum.Ok);
                await _dataFactoryService.CallWebhook(postObject, model.CallBackUri);
            }
            catch (Exception ex)
            {
                WebhookReturnPostObject postObject = new("Failed to call the domains to DataForSeo, see exception for more details", ex.ToString(), StatusCodeEnum.BadRequest);
                await _dataFactoryService.CallWebhook(postObject, model.CallBackUri);
            }
        }

        public async Task GetBulkRanks(DataForSeoBulkRanksPostModel model)
        {
            try
            {
                // Read domains for dim.domain (via ondemand database)
                var lDomains = await _domainOnDemandSqlService.GetFullTable("DataForSEOMergedSerpHouse", "dbo", new string[] { "domain" });

                var calltype = "bulk_ranks";

                Guid runid = Guid.NewGuid();
                List<string> lResponseJson = new List<string>();

                // Stuur telkens 100 domeinen richting de bulk                
                int allDomains = lDomains.Count;
                int page = 0;
                int maxDomainsPerCall = 1000;

                bool continuePaging = allDomains > 0;
                while (continuePaging)
                {
                    var bulkDomains = lDomains.Skip(page * maxDomainsPerCall).Take(maxDomainsPerCall).ToList();
                    var responseJson = await _dataForSeoService.BulkRanksLive(bulkDomains.Select(s => s[0]).ToList());
                    lResponseJson.Add(responseJson);

                    page++;
                    await SaveJsonsToTableStorage(calltype, runid, lResponseJson);
                    lResponseJson.Clear();
                    if ((page * maxDomainsPerCall) >= allDomains)
                        continuePaging = false;
                }

                if (lResponseJson.Count > 0)
                {
                    await SaveJsonsToTableStorage(calltype, runid, lResponseJson);
                }

                await SaveCallSettingsToTableStorage(calltype, runid, model);

                WebhookReturnPostObject postObject = new($"Successfully live called {lDomains.Count} domains to DataForSeo", string.Empty, StatusCodeEnum.Ok);
                await _dataFactoryService.CallWebhook(postObject, model.CallBackUri);
            }
            catch (Exception ex)
            {
                WebhookReturnPostObject postObject = new("Failed to call the domains to DataForSeo, see exception for more details", ex.ToString(), StatusCodeEnum.BadRequest);
                await _dataFactoryService.CallWebhook(postObject, model.CallBackUri);
            }
        }

        public async Task GetBulkReferringDomains(DataForSeoBulkReferringPostModel model)
        {
            try
            {
                // Read domains for dim.domain (via ondemand database)
                var lDomains = await _domainOnDemandSqlService.GetFullTable("DataForSEOMergedSerpHouse", "dbo", new string[] { "domain" });

                var calltype = "bulk_referring_domains";

                Guid runid = Guid.NewGuid();
                List<string> lResponseJson = new List<string>();

                // Stuur telkens 100 domeinen richting de bulk
                int allDomains = lDomains.Count;
                int page = 0;
                int maxDomainsPerCall = 1000;

                bool continuePaging = allDomains > 0;
                while (continuePaging)
                {

                    var bulkDomains = lDomains.Skip(page * maxDomainsPerCall).Take(maxDomainsPerCall).ToList();
                    var responseJson = await _dataForSeoService.BulkReferringDomains(bulkDomains.Select(s => s[0]).ToList());
                    lResponseJson.Add(responseJson);

                    page++;
                    await SaveJsonsToTableStorage(calltype, runid, lResponseJson);
                    lResponseJson.Clear();

                    if ((page * maxDomainsPerCall) >= allDomains)
                        continuePaging = false;
                }

                if (lResponseJson.Count > 0)
                {
                    await SaveJsonsToTableStorage(calltype, runid, lResponseJson);
                }

                await SaveCallSettingsToTableStorage(calltype, runid, model);

                WebhookReturnPostObject postObject = new($"Successfully live called {lDomains.Count} domains to DataForSeo", string.Empty, StatusCodeEnum.Ok);
                await _dataFactoryService.CallWebhook(postObject, model.CallBackUri);
            }
            catch (Exception ex)
            {
                WebhookReturnPostObject postObject = new("Failed to call the domains to DataForSeo, see exception for more details", ex.ToString(), StatusCodeEnum.BadRequest);
                await _dataFactoryService.CallWebhook(postObject, model.CallBackUri);
            }
        }

        public async Task GetBulkNewLost(DataForSeoBulkNewLostPostModel model)
        {
            try
            {
                // Read domains for dim.domain (via ondemand database)
                var lDomains = await _domainOnDemandSqlService.GetFullTable("DataForSEOMergedSerpHouse", "dbo", new string[] { "domain" });

                var calltype = "bulk_newlost";

                Guid runid = Guid.NewGuid();
                List<string> lResponseJson = new List<string>();


                // Stuur telkens 100 domeinen richting de bulk                
                int allDomains = lDomains.Count;
                int page = 0;
                int maxDomainsPerCall = 1000;

                bool continuePaging = allDomains > 0;
                while (continuePaging)
                {
                    var bulkDomains = lDomains.Skip(page * maxDomainsPerCall).Take(maxDomainsPerCall).ToList();
                    var responseJson = await _dataForSeoService.BulkNewLostDomains(bulkDomains.Select(s => s[0]).ToList(), model.FromMonth);
                    lResponseJson.Add(responseJson);

                    page++;
                    await SaveJsonsToTableStorage(calltype, runid, lResponseJson);
                    lResponseJson.Clear();
                    if ((page * maxDomainsPerCall) >= allDomains)
                        continuePaging = false;
                }

                if (lResponseJson.Count > 0)
                {
                    await SaveJsonsToTableStorage(calltype, runid, lResponseJson);
                }

                await SaveCallSettingsToTableStorage(calltype, runid, model);

                WebhookReturnPostObject postObject = new($"Successfully live called {lDomains.Count} domains to DataForSeo new lost", string.Empty, StatusCodeEnum.Ok);
                await _dataFactoryService.CallWebhook(postObject, model.CallBackUri);
            }
            catch (Exception ex)
            {
                WebhookReturnPostObject postObject = new("Failed to call the domains to DataForSeo new lost, see exception for more details", ex.ToString(), StatusCodeEnum.BadRequest);
                await _dataFactoryService.CallWebhook(postObject, model.CallBackUri);
            }
        }

        // Helper method to save JSONs to Table Storage
        private async Task SaveJsonsToTableStorage(string calltype, Guid runid, List<string> lResponseJson)
        {
            var dlDirectoryClient =
                await _dataLakeService.CreateNewDirectoryOrGetExisting($"raw/in/{connectorname}/{calltype}/{DateTime.Now.Year}/{DateTime.Now.Month}/{DateTime.Now.Day}/{runid}");
            // Sla alle JSONS op
            foreach (var json in lResponseJson)
            {
                await _dataLakeService.UploadRawData(dlDirectoryClient, json, Guid.NewGuid().ToString(), "json");
            }
        }

        // Helper method to save call settings to Table Storage
        private async Task SaveCallSettingsToTableStorage(string calltype, Guid runid, object model)
        {
            var dlDirectoryClient = await _dataLakeService.CreateNewDirectoryOrGetExisting($"raw/in/{connectorname}/{calltype}/{DateTime.Now.Year}/{DateTime.Now.Month}/{DateTime.Now.Day}/{runid}");
            // Sla alle JSONS op
            var json = JsonConvert.SerializeObject(model);
            await _dataLakeService.UploadRawData(dlDirectoryClient, json, "_settings", "json");
        }

        public async Task SaveTop100Queue(string runid, string connectorname, string calltype, string data)
        {
            var dlDirectoryClient = await _dataLakeService.CreateNewDirectoryOrGetExisting($"raw/in/{connectorname}/{calltype}/{DateTime.Now.Year}/{DateTime.Now.Month}/{DateTime.Now.Day}/{runid}");

            await _dataLakeService.UploadRawData(dlDirectoryClient, data, Guid.NewGuid().ToString(), "json");
        }

        public async Task SaveTop100Queue(string runid, string connectorname, string calltype, Stream data)
        {
            var dlDirectoryClient = await _dataLakeService.CreateNewDirectoryOrGetExisting($"raw/in/{connectorname}/{calltype}/{DateTime.Now.Year}/{DateTime.Now.Month}/{DateTime.Now.Day}/{runid}");

            await _dataLakeService.UploadRawData(dlDirectoryClient, data, Guid.NewGuid().ToString(), "gz");
        }

        public async Task RunTop100Live()
        {
            // Read keyword from table storage
            var lKeywords = _keywordTableStorageService.GetKeywords();

            foreach (var keywordModel in await lKeywords)
            {
                // Call DataForSeo
                var responseJson = await _dataForSeoService.GetTop100Refferers(keywordModel.Keyword);

                // Write JSON to DataLake
                var calltype = "live_regular";
                var dlDirectoryClient = await _dataLakeService.CreateNewDirectoryOrGetExisting($"raw/in/{connectorname}/{calltype}/{keywordModel.Category}/{keywordModel.SubCategory}/{keywordModel.SubSubCategory}/{DateTime.Now.Year}/{DateTime.Now.Month}/{DateTime.Now.Day}");
                await _dataLakeService.UploadRawData(dlDirectoryClient, responseJson, keywordModel.RowKey, "json");
            }
        }

        // Method to run on-page data retrieval
        public async Task RunOnPageData(DataForSeoOnPageModel model)

        {
            try

            {

                Dictionary<string, string> taskList = new Dictionary<string, string>();

                var domains = new string[] {

                                            "Triggre.com",

                                            "Smart-promotions.nl",

                                            "Vloerenmantegels.nl",

                                            "Tegelpaleis.nl",

                                            "Moneypenny.nl",

                                            "Hasci.nl",

                                            "Goossenswonen.nl",

                                            "Drone-zaak.nl",

                                            "Bloedwaardentest.nl",

                                            "711.nl",

                                            "Kansino.nl",

                                            "fairplaycasino.nl",

                                            "Bagageonline.nl",

                                            "Perfectviewcrm.nl",

                                            "Mijnlabtest.nl",

                                            "Body-supplies.nl",

                                            "Plent.nl",

                                            "Leef.nl",

                                            "Procardio.nl",

                                            "Wildstore.nl",

                                            "Partly.nl",

                                            "Fixje.nl",

                                            "Refurbished.nl"

                                };

                domains = model.domains;

                /// You can send up to 2000 API calls per minute ref: https://docs.dataforseo.com/v3/on_page/task_post/?csharp
                /// with each POST call containing no more than 100 tasks
                /// If your POST call contains over 100 tasks, the tasks over this limit will return the 40006 error.

                bool continuePaging = true;
                int tasksize = model.BatchSize;
                var page = 0;

                // Pauze for 1min after x amount of calls
                int pauzeAfterCalls = 2000;
                int dataForSeoPauzeTimeInMinutes = 1;
                Guid runid = Guid.NewGuid();

                // Door de minuut wachten en de skip take, kunnen we geen parallel foreach meer doen.
                while (continuePaging)
                {
                    var domains_for_call = domains.Skip(page * tasksize).Take(tasksize).ToArray();
                    if (domains_for_call.Count() < tasksize)
                        continuePaging = false;
                    if (domains_for_call.Count() > 0)

                    {
                        var taskIds = await _dataForSeoService.onpage_data_tasks(domains_for_call, 25000);

                        Task.Run(async () =>

                        {

                            Thread.Sleep(new TimeSpan(0, 1, 0));

                            var taskpage = await _dataForSeoService.onpage_data(taskIds);

                            foreach (var item in taskpage)

                            {

                                var calltype = "on_page";


                                var dlDirectoryClient = await _dataLakeService.CreateNewDirectoryOrGetExisting($"raw/in/{connectorname}/{calltype}/{item.Key}/{DateTime.Now.Year}/{DateTime.Now.Month}/{DateTime.Now.Day}");

                                await _dataLakeService.UploadRawData(dlDirectoryClient, item.Value, calltype, "json");

                            }

                        });

                    }

                    page++;

                    if (page % pauzeAfterCalls == 0)

                    {

                        Thread.Sleep(new TimeSpan(0, dataForSeoPauzeTimeInMinutes, 0));

                    }

                }

                if (!string.IsNullOrEmpty(model.CallBackUri))

                {

                    WebhookReturnPostObject postObject = new($"Successfully queue'd {domains.Length} domains to DataForSeo", string.Empty, StatusCodeEnum.Ok);

                    await _dataFactoryService.CallWebhook(postObject, model.CallBackUri);

                }

            }

            catch (Exception ex)

            {

                if (!string.IsNullOrEmpty(model.CallBackUri))

                {

                    WebhookReturnPostObject postObject = new("Failed to queue the domains to DataForSeo, see exception for more details", ex.ToString(), StatusCodeEnum.BadRequest);

                    await _dataFactoryService.CallWebhook(postObject, model.CallBackUri);

                }

            }

        }
    }
}