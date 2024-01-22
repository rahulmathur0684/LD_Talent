using Dorenbos_dataconnectors_library.Managers;
using Dorenbos_dataconnectors_library.Interfaces;
using Dorenbos_dataconnectors_library.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Threading;
using System.Threading.Tasks;

namespace DataConnectors_Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DataForSeoAPIController : ControllerBase
    {
        // Manager instance for handling SEO data operations
        private readonly IDataForSeoManager _dmanager;

        // Constructor with dependency injection for IDataForSeoManager
        public DataForSeoAPIController(IDataForSeoManager dataForSeoManager)
        {
            // Dependency injection
            _dmanager = dataForSeoManager;
        }

        // Endpoint to retrieve SERP data asynchronously
        [HttpPost("GetSerpData")]
        public async Task<IActionResult> GetSerpData([FromBody] DataForSeoSerpPostModel dataForSeoSerpPostModel)
        {
            // Start a new background thread to process the SERP data retrieval
            new Thread(() =>
            {
                Thread.CurrentThread.IsBackground = true;
                _dmanager.GetSerpData(dataForSeoSerpPostModel).Wait();
            }).Start();

            // Return an accepted response
            return Accepted();
        }

        // Endpoint to retrieve backlinks data asynchronously
        [HttpPost("GetBacklinks")]
        public async Task<IActionResult> GetBacklinks([FromBody] DataForSeoBacklinksPostModel dataForSeoBacklinksPostModel)
        {
            // Start a new background thread to process the backlinks data retrieval
            new Thread(() =>
            {
                Thread.CurrentThread.IsBackground = true;
                _dmanager.GetBacklinks(dataForSeoBacklinksPostModel).Wait();
            }).Start();

            // Return an accepted response
            return Accepted();
        }

        // Endpoint to retrieve bulk ranks data asynchronously
        [HttpPost("GetBulkRanks")]
        public async Task<IActionResult> GetBulkRanks([FromBody] DataForSeoBulkRanksPostModel dataForSeoBulkRanksPostModel)
        {
            // Start a new background thread to process the bulk ranks data retrieval
            new Thread(() =>
            {
                Thread.CurrentThread.IsBackground = true;
                _dmanager.GetBulkRanks(dataForSeoBulkRanksPostModel).Wait();
            }).Start();

            // Return an accepted response
            return Accepted();
        }

        // Endpoint to retrieve bulk referring domains data asynchronously
        [HttpPost("GetBulkReferringDomains")]
        public async Task<IActionResult> GetBulkReferringDomains([FromBody] DataForSeoBulkReferringPostModel dataForSeoBulkReferringPostModel)
        {
            // Start a new background thread to process the bulk referring domains data retrieval
            new Thread(() =>
            {
                Thread.CurrentThread.IsBackground = true;
                _dmanager.GetBulkReferringDomains(dataForSeoBulkReferringPostModel).Wait();
            }).Start();

            // Return an accepted response
            return Accepted();
        }

        // Endpoint to retrieve bulk new lost domains data asynchronously
        [HttpPost("GetBulkNewLostDomains")]
        public async Task<IActionResult> GetBulkNewLostDomains([FromBody] DataForSeoBulkNewLostPostModel dataForSeoBulknewLostPostModel)
        {
            // Start a new background thread to process the bulk new lost domains data retrieval
            new Thread(() =>
            {
                Thread.CurrentThread.IsBackground = true;
                _dmanager.GetBulkNewLost(dataForSeoBulknewLostPostModel).Wait();
            }).Start();

            // Return an accepted response
            return Accepted();
        }

        // Endpoint to retrieve on-page data asynchronously
        [HttpPost("GetOnPageData")]
        public async Task<IActionResult> GetOnPageData(DataForSeoOnPageModel dataforseoonpagemodel)
        {
            // Start a new background thread to process the on-page data retrieval
            new Thread(() =>
            {
                Thread.CurrentThread.IsBackground = true;
                _dmanager.RunOnPageData(dataforseoonpagemodel).Wait();
            }).Start();

            // Return an accepted response
            return Accepted();
        }
    }
}
