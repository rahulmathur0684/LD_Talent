using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.SqlServer;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web;
using Caterex.ExoWeb.App.Command.VisitSchedule;
using Caterex.ExoWeb.App.Data;
using Caterex.ExoWeb.App.Enums;
using Caterex.ExoWeb.App.Exceptions;
using Caterex.ExoWeb.App.Query.Customer.Partner;
using Caterex.ExoWeb.App.Query.Pricing;
using Caterex.ExoWeb.App.Query.Warehouse;

namespace Caterex.ExoWeb.App.Query.Customer
{
    public class DataForSeoQueryService 
    {
        private readonly IExoContextFactory exoContextFactory;
        private readonly CultureInfo culture;
        private readonly IRunsQueryService runsQueryService;
        private readonly Regex emailRegex = new Regex("^((([a-z]|\\d|[!#\\$%&'\\*\\+\\-\\/=\\?\\^_`{\\|}~]|[\\u00A0-\\uD7FF\\uF900-\\uFDCF\\uFDF0-\\uFFEF])+(\\.([a-z]|\\d|[!#\\$%&'\\*\\+\\-\\/=\\?\\^_`{\\|}~]|[\\u00A0-\\uD7FF\\uF900-\\uFDCF\\uFDF0-\\uFFEF])+)*)|((\\x22)((((\\x20|\\x09)*(\\x0d\\x0a))?(\\x20|\\x09)+)?(([\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x7f]|\\x21|[\\x23-\\x5b]|[\\x5d-\\x7e]|[\\u00A0-\\uD7FF\\uF900-\\uFDCF\\uFDF0-\\uFFEF])|(\\\\([\\x01-\\x09\\x0b\\x0c\\x0d-\\x7f]|[\\u00A0-\\uD7FF\\uF900-\\uFDCF\\uFDF0-\\uFFEF]))))*(((\\x20|\\x09)*(\\x0d\\x0a))?(\\x20|\\x09)+)?(\\x22)))@((([a-z]|\\d|[\\u00A0-\\uD7FF\\uF900-\\uFDCF\\uFDF0-\\uFFEF])|(([a-z]|\\d|[\\u00A0-\\uD7FF\\uF900-\\uFDCF\\uFDF0-\\uFFEF])([a-z]|\\d|-|\\.|_|~|[\\u00A0-\\uD7FF\\uF900-\\uFDCF\\uFDF0-\\uFFEF])*([a-z]|\\d|[\\u00A0-\\uD7FF\\uF900-\\uFDCF\\uFDF0-\\uFFEF])))\\.)+(([a-z]|[\\u00A0-\\uD7FF\\uF900-\\uFDCF\\uFDF0-\\uFFEF])|(([a-z]|[\\u00A0-\\uD7FF\\uF900-\\uFDCF\\uFDF0-\\uFFEF])([a-z]|\\d|-|\\.|_|~|[\\u00A0-\\uD7FF\\uF900-\\uFDCF\\uFDF0-\\uFFEF])*([a-z]|[\\u00A0-\\uD7FF\\uF900-\\uFDCF\\uFDF0-\\uFFEF])))\\.?$", RegexOptions.IgnoreCase | RegexOptions.ExplicitCapture | RegexOptions.Compiled);

        internal const string InvalidSizeUnit = "INVALID";
        internal const string DefaultSizeLabel = "Size";
        internal const string InternalStockCode = "X_FRT";

        public DataForSeoQueryService(IExoContextFactory exoContextFactory, CultureInfo culture, IRunsQueryService runsQueryService)
        {
            this.exoContextFactory = exoContextFactory;
            this.culture = culture;
            this.runsQueryService = runsQueryService;
        }

        public AddCustomerFormData GetAddCustomerFormData()
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                var associatedContacts = exo.DR_ACCS.Join(
                        exo.CONTACTS,
                        da => da.ACCNO,
                        co => co.COMPANY_ACCNO,
                        (da, co) => new { co, da })
                    .Where(x => x.co.COMPANY_ACCTYPE == 1)
                    .Select(x => new { x.co.SEQNO, x.co.FULLNAME, x.da.NAME })
                    .Distinct()
                    .OrderBy(x => x.FULLNAME)
                    .ThenBy(x => x.NAME)
                    .ToList();

                var customerModel = new AddCustomerFormData
                {
                    BuyingGroups = exo.DR_ACCS
                        .Select(x => x.X_BUYING_GROUP)
                        .Where(x => x != null)
                        .Distinct()
                        .ToList(),

                    CustomerIndustrys = GetAllIndustries(),

                    LeadSources = exo.X_Lead_Source
                        .Select(x => x.X_Source)
                        .Where(x => x != null)
                        .OrderBy(x => x)
                        .ToList(),

                    AssociatedContacts = associatedContacts
                        .Select(element => new { element.SEQNO, Name = HttpUtility.JavaScriptStringEncode(element.FULLNAME + ", " + element.NAME) })
                        .ToDictionary(x => x.SEQNO, x => x.Name)
                };

                return customerModel;
            }
        }

        public List<CustomerSearchResult> SearchCustomers(string searchTerm, bool includeClosedAccounts, bool searchBySuburbOrState, int[] excludeCustomerIds)
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                var query =
                    from c in exo.DR_ACCS
                    where c.ACCNO.ToString().Contains(searchTerm) || c.NAME.Contains(searchTerm)
                    where includeClosedAccounts || !c.NAME.Contains("Account closed")
                    select new CustomerSearchResult
                    {
                        AccNo = c.ACCNO,
                        Name = c.NAME ?? c.ACCNO.ToString(),
                        Suburb = c.ADDRESS3,
                        State = c.ADDRESS4,
                        Postcode = c.POST_CODE
                    };

                if (searchBySuburbOrState)
                {
                    query = query.Where(c => c.AccNo.ToString().Contains(searchTerm) || c.Name.Contains(searchTerm) || c.Suburb.Contains(searchTerm) || c.State.Contains(searchTerm));
                }
                else
                {
                    query = query.Where(c => c.AccNo.ToString().Contains(searchTerm) || c.Name.Contains(searchTerm));
                }

                if (!includeClosedAccounts)
                {
                    query = query.Where(c => !c.Name.Contains("Account closed"));
                }

                if (excludeCustomerIds != null && excludeCustomerIds.Length > 0)
                {
                    query = query.Where(c => !excludeCustomerIds.Contains(c.AccNo));
                }

                var result = query
                    .OrderBy(x => x.Name)
                    .Take(50)
                    .ToList();

                result.ForEach(x => x.Name = culture.TextInfo.ToTitleCase(x.Name.ToLower(culture)));

                return result;
            }
        }

        public (bool, string) CustomerOrderRefInfo(int customerId)
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                var x = exo.DR_ACCS.Where(a => a.ACCNO == customerId).Select(a => new { a.NEED_ORDERNO, a.NAME }).First();

                return (String.Equals(x.NEED_ORDERNO, "Y", StringComparison.OrdinalIgnoreCase), x.NAME);
            }
        }

        public async Task<bool> IsPastCustomerOrderReferenceUsed(int customerId, string customerRef)
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                return await exo.SALESORD_HDR.AnyAsync(so => so.ACCNO == customerId && so.CUSTORDERNO == customerRef.Trim());
            }
        }

        public async Task<Customer> GetCustomer(int customerAccountNo, DateTime now)
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                DateTime date = now.Date;
                var rc = await Task.Run(() => exo.X_GET_CUSTOMER(customerAccountNo, date).FirstOrDefault());

                string industry = string.Empty;
                string sizeLabel = DefaultSizeLabel;

                if (rc.X_BUSINESS_SUB_INDUSTRY.HasValue)
                {
                    X_BUSINESS_SUB_INDUSTRY bus = exo.X_BUSINESS_SUB_INDUSTRY.First(x => x.SEQNO == rc.X_BUSINESS_SUB_INDUSTRY.Value);
                    industry = bus.SEQNO.ToString();
                    if (!string.IsNullOrEmpty(bus.X_size_unit) && bus.X_size_unit != InvalidSizeUnit)
                    {
                        sizeLabel = bus.X_size_unit;
                    }
                }

                IList<CustomerContact> contacts = GetCustomerContacts(customerAccountNo);

                var dr = await Task.Run(() => exo.DR_ACCS.FirstOrDefault(x => x.ACCNO == rc.ACCNO));

                var customer = new Customer
                {
                    AccountGroupNo = rc.ACCGROUP,
                    DeliveryRunId = rc.X_DELRUN ?? 0,
                    Size = rc.X_SIZE2,
                    SizeLabel = sizeLabel,
                    Industry = industry,
                    Email = rc.EMAIL,
                    CustomerTradingName = rc.NAME,
                    Abn = rc.TAXREG,
                    AccountActive = rc.ISACTIVE == "Y",
                    AccountNumber = rc.ACCNO,
                    AdditionalAddressing = rc.ADDRESS1,
                    PostalAddress = rc.ADDRESS2,
                    PostalSuburb = rc.ADDRESS3,
                    PostalState = rc.ADDRESS4,
                    PostalPostcode = rc.POST_CODE,
                    BuyingGroup = rc.X_BUYING_GROUP,
                    DeliveryIdentifier = rc.DELADDR1,
                    DeliveryAddress = rc.DELADDR2,
                    DeliveryAdditional = rc.DELADDR3,
                    DeliverySuburb = rc.DELADDR4,
                    DeliveryState = rc.DELADDR5,
                    DeliveryPostcode = rc.DELADDR6,
                    Fax = rc.FAX,
                    Telephone = rc.PHONE,
                    LegalEntity = rc.X_LEGAL,
                    DeliveryNotes = rc.X_NOTE_DELIVERY,
                    DefaultContactId = rc.CONTACT_SEQNO,
                    SalespersonId = rc.SalespersonId ?? 0,
                    Salesperson = rc.Salesperson,
                    AccountBalance = rc.BALANCE ?? 0,
                    CreditStatus = rc.STOPCREDIT != "N"
                        ? CreditStatus.StopCredit
                        : rc.IsOverdue > 0
                            ? CreditStatus.Overdue
                            : CreditStatus.Ok,
                    MarketingQuarantine = rc.X_MKT_COMM_BLACKOUT == "Y",
                    OverdueBalance = rc.OverdueBalance ?? 0,
                    WebAccess = rc.X_WEB == "Y",
                    Color = rc.X_COLOUR,
                    LastTeleCall = rc.X_CALL_LAST,
                    EnforceOrderNumber = rc.NEED_ORDERNO == "Y",
                    EmailPriceUpdateStatus = rc.X_EMAIL_UPDATES != "N",
                    
                    IsInsertSaleseNo = dr.X_COMMS_INSERT_SALESNO == null
                        ? new bool?()
                        : dr.X_COMMS_INSERT_SALESNO != "N",

                    IsInsertHelpNo = dr.X_COMMS_INSERT_HELPNO != "N",
                    IsUpdateSaleseNo = dr.X_COMMS_UPDATE_SALESNO != "N",
                    IsUpdateHelpNo = dr.X_COMMS_UPDATE_HELPNO != "N",
                    StatementDeliveryMethod = rc.STATEMENT == "P" ? DeliveryMethod.Post : DeliveryMethod.Email,
                    LastSiteVisit = rc.X_VISIT_LAST,
                    LastHistoryNote = rc.LastContactHist.HasValue && rc.LastContactHist > rc.LastDrContHist
                        ? rc.LastContactHist
                        : rc.LastDrContHist,
                    Contacts = contacts,
                    EmailContacts = contacts
                        .Where(x => x.Email != null && emailRegex.IsMatch(x.Email))
                        .ToList(),
                    StatementEmailContactId = rc.STATEMENT_CONTACT_SEQNO == -1 ? null : rc.STATEMENT_CONTACT_SEQNO,
                    PriceLevel = rc.PriceLevel,
                    Rating = rc.X_RATING,
                    CustomerName = (rc.X_ULT_CUST.HasValue && rc.ACCNO != rc.X_ULT_CUST) ? exo.DR_ACCS.Select(a => new { a.ACCNO, a.NAME }).First(x => x.ACCNO == rc.X_ULT_CUST).NAME : "",
                    FieldServiceBy = rc.X_FSS,
                    ServiceInterval = rc.X_SERVICE_INTERVAL,
                    NextFreeDelivery = runsQueryService.GetNextFreeDelivery(rc.X_DELRUN),
                    IsHeadOffice = rc.HEAD_ACCNO == -1 && exo.DR_ACCS.Any(x => x.HEAD_ACCNO == rc.ACCNO),
                    HeadOfficeCreditStatus = exo.DR_ACCS.Any(x => x.ACCNO == rc.HEAD_ACCNO && x.STOPCREDIT == "Y")
                        ? CreditStatus.StopCredit
                        : CreditStatus.Ok,
                    RunOrder = dr.X_RUNORDER,
                    Alert = dr.x_alert,
                    FisOver = dr.X_FIS,
                    InvoiceWithGoods = (dr.X_INV_WITH_GOODS ?? "") == "Y"
                };

                return customer;
            }
        }

        public IList<CustomerContact> GetCustomerContacts(int customerAccountNo)
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                var contactsQuery =
                    from x in exo.DR_CONTACTS
                    join da in exo.DR_ACCS on x.ACCNO equals da.ACCNO
                    where x.ACCNO == customerAccountNo
                    select new
                    {
                        x.CONTACT.SEQNO,
                        x.CONTACT.FIRSTNAME,
                        x.CONTACT.LASTNAME,
                        x.CONTACT.EMAIL,
                        x.CONTACT.MOBILE,
                        x.CONTACT.NOTES,
                        x.CONTACT.TITLE,
                        x.CONTACT.DIRECTPHONE,
                        x.DEFCONTACT,
                        x.CONTACT.COMPANY_ACCTYPE,
                        da.STATEMENT_CONTACT_SEQNO,
                        x.CONTACT.X_AUTODOC_METHOD,
                        x.CONTACT_SEQNO
                    };

                var symbols = exo.Database.SqlQuery<DictionaryResult>("SELECT [SEQNO] AS [Key], [SYMBOL] AS [Value] FROM [dbo].[X_AUTODOC_METHOD]")
                        .ToList();

                var contacts = contactsQuery
                    .ToList()
                    .Select(x => new CustomerContact
                    {
                        Id = x.SEQNO,
                        Name = x.FIRSTNAME?.Trim() + " " + x.LASTNAME?.Trim(),
                        Email = x.EMAIL,
                        Title = x.TITLE,
                        Mobile = x.MOBILE,
                        Phone = x.DIRECTPHONE,
                        Notes = x.NOTES,
                        IsDefaultContact = x.DEFCONTACT == "Y",
                        IsStatementContact = x.STATEMENT_CONTACT_SEQNO == x.SEQNO,
                        CompanyType = (CompanyType?)x.COMPANY_ACCTYPE,
                        AD = x.X_AUTODOC_METHOD.HasValue ? symbols.FirstOrDefault(s => s.Key == x.X_AUTODOC_METHOD.Value)?.Value : null,
                        ContactSeqNo = x.CONTACT_SEQNO
                    })
                    .OrderByDescending(x => x.IsDefaultContact)
                    .ThenByDescending(x => x.IsStatementContact)
                    .ThenBy(x => x.Name)
                    .ToList();

                return contacts;
            }
        }

        public IDictionary<int, string> GetAllServiceIntervals()
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                return exo.X_Intervals
                    .ToDictionary(
                        x => x.Service,
                        x => x.Service.ToString()
                    );
            }
        }

        public IList<BudgetInfo> GetBudgetInfo(int drAccNo, bool isHeadOffice)
        {
            DateTime startOfThisMonth = DateTime.Today.AddDays(-DateTime.Today.Day + 1);

            DateTime startDate = startOfThisMonth.AddMonths(-12);
            DateTime endDate = startOfThisMonth.AddMonths(12);

            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                var branchAccountNumbers = new List<string>();
                if (isHeadOffice)
                {
                    branchAccountNumbers = exo.DR_ACCS.Where(a => a.HEAD_ACCNO == drAccNo).Select(a => a.ACCNO.ToString()).ToList();
                }

                var results =
                    from target in exo.X_TARGETS.Where(t => t.TARGETTYPE_SEQNO == 10 && t.TARGET_SEQNO == drAccNo.ToString())
                    join period in exo.MANREP_PERIOD on target.PERIOD_SEQNO equals period.PERIOD_SEQNO
                    where period.STARTDATE >= startDate && period.STARTDATE <= endDate
                    select new BudgetInfo
                    {
                        AccountNumber = drAccNo,
                        PeriodSeqNo = target.PERIOD_SEQNO,
                        TargetSeqNo = target.SEQNO,
                        PeriodStartDate = period.STARTDATE.Value,
                        BudgetValue = isHeadOffice ? (target.VALUE ?? 0) + exo.X_TARGETS.Where(xt => branchAccountNumbers.Contains(xt.TARGET_SEQNO) &&
                                                                            xt.PERIOD_SEQNO == target.PERIOD_SEQNO).Sum(xt => xt.VALUE ?? 0)
                                                   : (target.VALUE ?? 0),
                        ActualValue = exo.DR_TRANS
                                         .Where(t => t.SALES_ACCNO == drAccNo && t.TRANSDATE >= period.STARTDATE && t.TRANSDATE <= period.ENDDATE)
                                         .Sum(l => l.SUBTOTAL ?? 0)
                    };

                return results.OrderBy(p => p.PeriodSeqNo)
                              .ToList();
            }
        }

        public async Task<byte[]> GetContractAttachment(int seqNo)
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                var data = await exo.X_CONTRACT
                    .Select(x => new { x.SEQNO, x.DOCDATA })
                    .SingleAsync(s => s.SEQNO == seqNo);

                return data.DOCDATA;
            }
        }

        public IDictionary<string, string> GetAllIndustries()
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                return exo.X_BUSINESS_SUB_INDUSTRY.Where(b => b.Sort < 99).OrderBy(b => b.Sort).ToDictionary(s => s.SEQNO.ToString(), s => s.DESCRIPTION);
            }
        }

        public IList<string> SearchBuyingGroups(string searchTerm)
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                return exo.DR_ACCS
                    .Select(x => x.X_BUYING_GROUP)
                    .Where(x => x.Contains(searchTerm))
                    .Take(10)
                    .Distinct()
                    .ToList();
            }
        }

        public PriceRulesAvailable GetPriceRulesAvailable(int customerAccountNo)
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                var acc = exo.DR_ACCS.Single(x => x.ACCNO == customerAccountNo);

                return new PriceRulesAvailable
                {
                    FunctionAvailable = true,
                    CanCreate = acc.PRICENO != 3
                };
            }
        }

        public async Task<int> GetCustomerDefaultLocation(int customerAccountNo)
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                return (await exo.DR_ACCS.Where(s => s.ACCNO == customerAccountNo)
                    .Select(d => d.X_DEFLOC)
                    .FirstOrDefaultAsync()).GetValueOrDefault();
            }
        }

        public DebitAccount GetCustomerAutofillData(int customerAccountNo)
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                DR_ACCS drAccs = exo.DR_ACCS.Single(x => x.ACCNO == customerAccountNo);
                if (drAccs != null)
                {
                    return new DebitAccount
                    {
                        AdditionalAddressing = drAccs.ADDRESS1,
                        PostalAddress = drAccs.ADDRESS2,
                        PostalSuburb = drAccs.ADDRESS3,
                        PostalState = drAccs.ADDRESS4,
                        PostalPostcode = drAccs.POST_CODE,
                        Email = drAccs.EMAIL,
                        Telephone = drAccs.PHONE,
                        Fax = drAccs.FAX,
                        DeliveryIdentifier = drAccs.DELADDR1,
                        DeliveryAddress = drAccs.DELADDR2,
                        DeliveryAdditional = drAccs.DELADDR3,
                        DeliverySuburb = drAccs.DELADDR4,
                        DeliveryState = drAccs.DELADDR5,
                        DeliveryPostcode = drAccs.DELADDR6
                    };
                }
                return null;
            }
        }

        public async Task<Contact> GetContact(int contactId)
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                // ReSharper disable once AccessToDisposedClosure
                var contact =
                     await (from c in exo.CONTACTS
                            let da = exo.DR_ACCS.FirstOrDefault(x => x.ACCNO == c.COMPANY_ACCNO)
                            where c.SEQNO == contactId && c.COMPANY_ACCTYPE == (int)CompanyType.Customer
                            select new Contact
                            {
                                X_Tpb = c.X_TPB,
                                FirstName = c.FIRSTNAME,
                                LastName = c.LASTNAME,
                                Title = c.TITLE,
                                RoleName = c.X_ROLE,
                                AdditionalAddressing = c.ADDRESS1,
                                PostalAddress = c.ADDRESS2,
                                PostalSuburb = c.ADDRESS3,
                                PostalState = c.ADDRESS4,
                                PostalPostcode = c.POST_CODE,
                                DeliveryIdentifier = c.DELADDR1,
                                DeliveryAddress = c.DELADDR2,
                                DeliveryAdditional = c.DELADDR3,
                                DeliverySuburb = c.DELADDR4,
                                DeliveryState = c.DELADDR5,
                                DeliveryPostcode = c.DELADDR6,
                                Mobile = c.MOBILE,
                                Telephone = c.DIRECTPHONE,
                                Email = c.EMAIL,
                                CustomerAccountNumber = da == null ? (int?)null : da.ACCNO,
                                CustomerTradingName = da == null ? null : da.NAME,
                                ContactId = c.SEQNO,
                                Notes = c.NOTES,
                                WebAccess = c.X_WEB == "Y",
                                WebUserType = c.X_WEB_TYPE,
                                LimitItems = c.X_WEBVIEW_LIMIT == "Y",
                                LimitSpend = c.X_AUTH_LIMIT,
                                Marketing = c.X_TELESALES != "N",
                                EMarketingOptOut = c.OPTOUT_EMARKETING == "Y",
                                Sub4 = c.SUB4 == "Y",
                                NextDelivery = c.X_COMM_DEL != "N",
                                DueSoon = c.X_COMM_REM != "N",
                                PaysAccount = c.X_PAYS_ACCOUNT != "N",
                                Birthdate = c.X_BDAY,
                                BirthdayMessage = c.X_BD_TRACK == "Y",
                                PriorityContact = c.X_HVT == "Y",
                                EmailConfirmedAt = c.X_CKD_EMAIL,
                                DirectPhoneConfirmedAt = c.X_CKD_DIRECTPHONE,
                                MobileConfirmedAt = c.X_CKD_MOBILE,
                                RoleConfirmedAt = c.X_CKD_ROLE,
                                TitleConfirmedAt = c.X_CKD_TITLE,
                                InvoiceDeliveryType = c.X_AUTODOC_METHOD != null ? c.X_AUTODOC_METHOD.ToString() : null,
                            })
                    .FirstOrDefaultAsync();

                if (contact != null)
                {
                    var daysSinceContactInfoChecked = await exo.X_ExoWeb
                         .Where(x => x.Feature.Trim() == DatabaseConstants.FunctionSettings.DaysSinceContactInfoChecked)
                         .Select(x => x.Value.Value)
                         .SingleAsync();

                    contact.ContactInfoCheckedDate = DateTime.Now.AddDays(-daysSinceContactInfoChecked);

                    var contactLinksQuery =
                        from c in exo.CONTACTS
                        join dc in exo.DR_CONTACTS on c.SEQNO equals dc.CONTACT_SEQNO
                        join da in exo.DR_ACCS on dc.ACCNO equals da.ACCNO
                        where c.SEQNO == contactId && c.COMPANY_ACCTYPE == (int)CompanyType.Customer
                        select new ContactLink
                        {
                            ContactLinkId = dc.SEQNO,
                            CustomerId = da.ACCNO,
                            Customer = da.ACCNO + " " + da.NAME,
                            Primary = dc.ACCNO == c.COMPANY_ACCNO,
                            MainContact = dc.DEFCONTACT == "Y",
                            CanRemoveLink = exo.DR_CONTACTS.Count(link => link.ACCNO == da.ACCNO) > 1 && dc.DEFCONTACT != "Y"
                        };

                    contact.Links = await contactLinksQuery.ToListAsync();
                }
                else
                {
                    contact = new Contact { Links = new List<ContactLink>() };
                }

                return contact;
            }
        }

        public async Task<DateTime> UpdateTransactionDueDate(int transactionId)
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                var transaction = await exo.DR_TRANS.FirstOrDefaultAsync(t => t.SEQNO == transactionId);

                if (transaction.DUEDATE.HasValue)
                {
                    transaction.DUEDATE = transaction.DUEDATE.Value.AddDays(14);
                    transaction.X_DUEDATE_EXTENDED_ON = DateTime.Now;
                    await exo.SaveChangesAsync();

                    return transaction.DUEDATE.Value;
                }
            }
            return DateTime.MinValue;
        }

        public IList<CustomerTransaction> GetTransactions(int loggedInStaffNo, int customerAccountNo, DateTime now)
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                DateTime date = now.Date;

                var rawCustomers =
                    from da in exo.DR_ACCS
                    select new
                    {
                        da.ACCNO,
                        da.SALESNO,
                        da.X_EZC_PAYMENT_LINK,
                        da.HEAD_ACCNO,
                        da.KEEPTRANSACTIONS,
                        Transactions = da.DR_TRANS
                            .Where(x => SqlFunctions.DateDiff("dd", date, x.TRANSDATE) <= 91)
                            .OrderByDescending(x => x.TRANSDATE)
                            .Select(x => new
                            {
                                x.SEQNO,
                                x.AMOUNT,
                                x.TRANSDATE,
                                x.DUEDATE,
                                x.X_DUEDATE_EXTENDED_ON,
                                x.INVNO,
                                x.ALLOCATEDBAL,
                                x.ALLOCATED,
                                x.REF1,
                                x.REF2,
                                x.TRANSTYPE,
                                x.DISPATCH_INFO,
                                x.X_CON_NOTE
                            })
                    };

                var rc = rawCustomers.First(x => x.ACCNO == customerAccountNo);
                Func<int?, double?, string, TransactionType> getTransactionType = (transType, amount, allocated) =>
                {
                    switch (transType)
                    {
                        case 1:
                            return amount < 0 ? TransactionType.Credit : TransactionType.Invoice;
                        case 4:
                            return TransactionType.Payment;
                        case 5:
                            return TransactionType.Adjustment;
                        default:
                            throw new ExoIntegrityException($"Unexpected value of {transType} for TRANSTYPE");
                    }
                };

                var paymentLink = rc.X_EZC_PAYMENT_LINK;
                if (rc.HEAD_ACCNO != -1 && rc.KEEPTRANSACTIONS == "N")
                {
                    var headAcc = exo.DR_ACCS.Single(da => da.ACCNO == rc.HEAD_ACCNO);

                    paymentLink = headAcc.X_EZC_PAYMENT_LINK;
                }

                IEnumerable<CustomerTransaction> transactions = rc.Transactions
                    .Select(x =>
                    {
                        int xConNote;
                        int.TryParse(x.X_CON_NOTE, out xConNote);

                        return new CustomerTransaction
                        {
                            Id = x.SEQNO,
                            Amount = x.AMOUNT,
                            Date = x.TRANSDATE,
                            DueDate = x.DUEDATE,
                            InvoiceNumber = x.INVNO,
                            Outstanding = x.AMOUNT - x.ALLOCATEDBAL,
                            Reference1 = x.REF1,
                            Reference2 = x.REF2,
                            Transaction = getTransactionType(x.TRANSTYPE, x.AMOUNT, x.ALLOCATED),
                            DueDatendedOn = x.X_DUEDATE_EXTENDED_ON,
                            StaffNo = rc.SALESNO,
                            LoggedInStaffNo = loggedInStaffNo,
                            PaymentLink = paymentLink,
                            CanNoteId = x.X_CON_NOTE,
                            DisplayTransaction = x.ALLOCATED == "0",
                            DisplayTrack = (x.DISPATCH_INFO?.Equals("FEDEX") ?? false) && xConNote > 0
                        };
                    });

                return transactions.ToList();
            }
        }

        public async Task<AddContactFormData> GetAddContactFormData(int accountNumber)
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                var drAccs = await exo.DR_ACCS.SingleAsync(x => x.ACCNO == accountNumber);

                var formData = new AddContactFormData
                {
                    CustomerTradingName = drAccs.NAME,
                    CustomerAccountNumber = accountNumber,
                    AdditionalAddressing = drAccs.ADDRESS1,
                    PostalAddress = drAccs.ADDRESS2,
                    PostalSuburb = drAccs.ADDRESS3,
                    PostalState = drAccs.ADDRESS4,
                    PostalPostcode = drAccs.POST_CODE,
                    DeliveryIdentifier = drAccs.DELADDR1,
                    DeliveryAddress = drAccs.DELADDR2,
                    DeliveryAdditional = drAccs.DELADDR3,
                    DeliverySuburb = drAccs.DELADDR4,
                    DeliveryState = drAccs.DELADDR5,
                    DeliveryPostcode = drAccs.DELADDR6,
                    Email = drAccs.EMAIL,
                    Telephone = drAccs.PHONE,
                    FirstNames = exo.CONTACTS
                        .Where(x => x.COMPANY_ACCNO == drAccs.ACCNO && x.COMPANY_ACCTYPE == 1 && x.FIRSTNAME != null)
                        .Select(x => x.FIRSTNAME)
                        .ToList(),
                    WebAccess = false,
                    LimitItems = false,
                    LimitSpend = 5000,
                    IsUniequeEmail = true
                };

                return formData;
            }
        }

        public IList<ContactCompany> SearchContacts(string searchTerm, int excludeContactsLinkedToThisAccountNo)
        {
            return SearchContacts(searchTerm, (int?)excludeContactsLinkedToThisAccountNo);
        }

        public IList<ContactCompany> SearchContacts(string searchTerm, int? excludeContactsLinkedToThisAccountNo)
        {
            return SearchContacts(searchTerm, excludeContactsLinkedToThisAccountNo, null);
        }

        public IList<ContactCompany> SearchContacts(string searchTerm, int? excludeContactsLinkedToThisAccountNo, int? accNo)
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                IQueryable<SearchContactsResult> contacts =
                    from c in SearchContactsQuery(exo)
                    where excludeContactsLinkedToThisAccountNo == null
                          || !c.DrContacts.Select(y => y.ACCNO).Contains(excludeContactsLinkedToThisAccountNo)
                    where c.FullName.Contains(searchTerm)
                    select c;

                if (accNo.HasValue)
                {
                    //join dc in exo.DR_CONTACTS on c.Id equals dc.CONTACT_SEQNO
                    IQueryable<DR_CONTACTS> drContacts = exo.DR_CONTACTS.Where(x => x.ACCNO == accNo.Value);

                    contacts = contacts.Join(drContacts, outer => outer.Id,
                        inner => inner.CONTACT_SEQNO, (result, _) => result);
                }

                return contacts
                    .OrderBy(c => c.LastName)
                    .ThenBy(c => c.FirstName)
                    .Take(15)
                    .ToList()
                    .Select(x => new ContactCompany
                    {
                        ContactId = x.Id,
                        ContactName = culture.TextInfo.ToTitleCase(x.FullName.ToLower(culture)),
                        CompanyName = x.CompanyName == null ? null : culture.TextInfo.ToTitleCase(x.CompanyName.ToLower(culture)),
                        CompanyType = x.CompanyType
                    })
                    .ToList();
            }
        }

        private static IQueryable<SearchContactsResult> SearchContactsQuery(ExoEntities exo)
        {
            IQueryable<SearchContactsResult> contacts =
                from c in exo.CONTACTS
                let da = exo.DR_ACCS.FirstOrDefault(x => x.ACCNO == c.COMPANY_ACCNO)
                let ca = exo.CR_ACCS.FirstOrDefault(x => x.ACCNO == c.COMPANY_ACCNO)
                select new SearchContactsResult
                {
                    Id = c.SEQNO,
                    FullName = c.FULLNAME == null ? c.FIRSTNAME + " " + c.LASTNAME : c.FULLNAME,
                    FirstName = c.FIRSTNAME,
                    LastName = c.LASTNAME,
                    DrContacts = c.DR_CONTACTS,
                    CompanyAccountNumber = c.COMPANY_ACCNO,
                    CompanyType = (CompanyType?)c.COMPANY_ACCTYPE,
                    CompanyName = c.COMPANY_ACCTYPE == (int)CompanyType.Customer
                        ? da.NAME
                        : c.COMPANY_ACCTYPE == (int)CompanyType.Supplier
                            ? ca.NAME
                            : null
                };
            return contacts;
        }

        private class SearchContactsResult
        {
            public int Id { get; set; }
            public string FullName { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public CompanyType? CompanyType { get; set; }
            public string CompanyName { get; set; }
            public ICollection<DR_CONTACTS> DrContacts { get; set; }
            public int? CompanyAccountNumber { get; set; }
        }

        public IDictionary<int, string> ListContactsForCustomerHistoryNote(int customerAccountNumber)
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                IQueryable<SearchContactsResult> contacts =
                    from c in SearchContactsQuery(exo)
                    join dc in exo.DR_CONTACTS on c.Id equals dc.CONTACT_SEQNO
                    where dc.ACCNO == customerAccountNumber
                    select c;

                return contacts
                    .Take(15)
                    .ToDictionary(
                        x => x.Id,
                        x => culture.TextInfo.ToTitleCase(x.FullName.ToLower(culture))
                    );
            }
        }

        public IList<DuplicateContactResult> CheckForExistingContacts(string firstName, string lastName)
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                IOrderedQueryable<SearchContactsResult> matches =
                    from dc in SearchContactsQuery(exo)
                    where dc.FirstName.Equals(firstName) &&
                          dc.LastName.Equals(lastName)
                    orderby dc.CompanyName
                    select dc;

                return matches
                    .AsEnumerable()
                    .Select(x => new DuplicateContactResult
                    {
                        Id = x.Id,
                        Name = x.FullName,
                        CompanyName = x.CompanyName,
                        CompanyType = x.CompanyType
                    })
                    .ToList();
            }
        }

        public IList<CustomerQuote> GetStaffQuotes(int staffNumber)
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                IQueryable<CustomerQuote> customerQuotes =
                    from salesOrder in exo.SALESORD_HDR
                    join customer in exo.DR_ACCS on salesOrder.ACCNO.Value equals customer.ACCNO
                    where (salesOrder.STATUS == 3) && customer.SALESNO.Value == staffNumber
                    orderby salesOrder.SEQNO
                    select new CustomerQuote
                    {
                        Id = salesOrder.SEQNO,
                        CustomerName = customer.NAME,
                        QuoteDate = salesOrder.ORDERDATE,
                        ExpiryDate = DateTime.Now,
                        Reference = salesOrder.CUSTORDERNO,
                        QuoteValue = salesOrder.SUBTOTAL + salesOrder.TAXTOTAL
                    };

                var quotes = customerQuotes.ToList();
                quotes.ForEach(q =>
                {
                    q.ExpiryDate = GetLastCalendarDay(q.QuoteDate.Value);
                });
                return quotes;
            }
        }

        public async Task<SlimCustomerQuote> GetCustomerQuoteDetails(int quoteId)
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                IQueryable<CustomerQuoteDetailLine> quoteDetails =
                    from quoteLine in exo.SALESORD_LINES
                    join stockItem in exo.STOCK_ITEMS on quoteLine.STOCKCODE equals stockItem.STOCKCODE
                    join stockc in exo.STOCK_CLASSIFICATIONS on stockItem.STOCK_CLASSIFICATION equals stockc.CLASSNO
                    join stock in exo.STOCK_LOC_INFO on new { Loc = quoteLine.LOCATION.Value, quoteLine.STOCKCODE } equals new { Loc = stock.LOCATION, stock.STOCKCODE }
                    where quoteLine.HDR_SEQNO == quoteId
                    select new CustomerQuoteDetailLine
                    {
                        StockCode = quoteLine.STOCKCODE,
                        Description = quoteLine.DESCRIPTION,
                        QuotedQty = quoteLine.ORD_QUANT,
                        QuotedPrice = quoteLine.LINETOTAL.Value / quoteLine.ORD_QUANT.Value,
                        QuoteValue = quoteLine.LINETOTAL,
                        QuoteTotal = quoteLine.LINETOTAL.Value * (100 + quoteLine.TAXRATE.Value) / 100,
                        Stock = stock.X_Stock,
                        Location = stock.LOCATION,
                        StockCodeColour = stockc.X_COLOUR
                    };

                var customerQuoteDetailLines = await quoteDetails.ToListAsync();
                customerQuoteDetailLines.ForEach(q =>
                {
                    q.Avilability = exo.STOCK_AVAILABILITY_FN(q.StockCode, q.Location).FirstOrDefault();
                });

                var lostReasons = await exo.Database.SqlQuery<DictionaryResult>("SELECT [SEQNO] AS [Key], [DESCRIPTION] AS [Value] FROM [dbo].[X_LOST_REASON_TBL]")
                    .ToDictionaryAsync(k => k.Key.ToString(), v => v.Value);

                var quote = exo.SALESORD_HDR.FirstOrDefault(ss => ss.SEQNO == quoteId);

                return new SlimCustomerQuote
                {
                    QuoteId = quoteId,
                    QuoteDate = quote.CREATE_DATE,
                    QuoteExpiryDate = quote.DUEDATE,
                    QuoteReference = quote.REFERENCE,
                    Details = customerQuoteDetailLines,
                    LostReasons = lostReasons
                };
            }
        }

        DateTime GetLastCalendarDay(DateTime dateWithinQuarter)
        {
            var year = dateWithinQuarter.Year;
            if (dateWithinQuarter.Month <= 3)
            {
                return new DateTime(year, 3, DateTime.DaysInMonth(year, 3));
            }
            if (dateWithinQuarter.Month <= 6)
            {
                return new DateTime(year, 6, DateTime.DaysInMonth(year, 6));
            }

            if (dateWithinQuarter.Month <= 9)
            {
                return new DateTime(year, 9, DateTime.DaysInMonth(year, 9));
            }

            return new DateTime(year, 12, DateTime.DaysInMonth(year, 12));
        }

        public IList<SalesOrderLineItem> GetSalesOrderLineDetails(int salesOrderId)
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                var results =
                    from result in exo.GetSalesOrderLines(salesOrderId)
                    join stockitem in exo.STOCK_ITEMS on result.StockCode equals stockitem.STOCKCODE
                    join stockc in exo.STOCK_CLASSIFICATIONS on stockitem.STOCK_CLASSIFICATION equals stockc.CLASSNO
                    select new SalesOrderLineItem
                    {
                        DueDate = result.Outstanding == 0 ? null : result.DueDate,
                        Description = result.Description,
                        Outstanding = result.Outstanding,
                        Status = result.Outstanding == 0 ? "Fulfilled" : result.Status,
                        Ordered = result.Ordered,
                        StockCode = result.StockCode,
                        StockCodeColour = stockc.X_COLOUR
                    };

                return results.ToList();
            }
        }

        public IList<SalesOrder> GetSalesOrdersForStaff(int staffNo)
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                IQueryable<SalesOrder> salesOrders =
                    from order in exo.SALESORD_HDR
                    join c in exo.DR_ACCS on order.ACCNO.Value equals c.ACCNO
                    where (order.STATUS == 1 || order.STATUS == 0) && c.SALESNO.Value == staffNo
                    orderby order.SEQNO
                    select new SalesOrder
                    {
                        Id = order.SEQNO,
                        Name = c.NAME,
                        OrderDate = order.CREATE_DATE,
                        DueDate = order.DUEDATE,
                        CustomerOrderNumber = order.CUSTORDERNO,
                        Subtotal = order.SUBTOTAL,
                        TotalTax = order.TAXTOTAL,
                        AccNo = order.ACCNO.Value
                    };

                return salesOrders.ToList();
            }
        }

        public async Task<SalesOrderLines> GetQuoteSalesOrderLines(int accNo, int status = 0)
        {
            var pqs = new PricingQueryService(exoContextFactory);
            var priceRules = await pqs.GetPriceRules(accNo, DateTime.Now);
            var priceRulesAvailable = GetPriceRulesAvailable(accNo);
            IEnumerable<SalesOrderLine> salesOrderLines = await GetSalesOrderLines(accNo, status);

            return new SalesOrderLines(accNo, priceRulesAvailable.FunctionAvailable, priceRulesAvailable.CanCreate, priceRules, salesOrderLines);
        }

        public async Task<IEnumerable<SalesOrderLine>> GetSalesOrderLines(int customerId, int status = 0)
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                IQueryable<SALESORD_HDR> orders = status <= 0
                    ? exo.SALESORD_HDR.Where(order => order.STATUS == 0 || order.STATUS == 1)
                    : exo.SALESORD_HDR.Where(order => order.STATUS == status);

                var bannedStockCodes = exo.X_BANNED.Where(b => b.ACCNO == customerId).Select(b => b.STOCKCODE).ToList();

                var salesOrderLines =
                    from line in exo.SALESORD_LINES
                    join stockItem in exo.STOCK_ITEMS on line.STOCKCODE equals stockItem.STOCKCODE
                    join stockc in exo.STOCK_CLASSIFICATIONS on stockItem.STOCK_CLASSIFICATION equals stockc.CLASSNO
                    join order in orders on line.HDR_SEQNO equals order.SEQNO
                    join accs in exo.DR_ACCS on line.ACCNO equals accs.ACCNO
                    where order.ACCNO == customerId && line.UNSUP_QUANT > 0 && !bannedStockCodes.Contains(line.STOCKCODE)
                    orderby order.SEQNO
                    select new
                    {
                        HDR_SEQNO = order.SEQNO,    
                        Status = order.X_MSP_AWMS_SO_ProgressNo,
                        line.SEQNO,
                        line.STOCKCODE,
                        line.DESCRIPTION,
                        line.X_ENTERED,
                        line.DUEDATE,
                        line.UNSUP_QUANT,
                        line.UNITPRICE,
                        line.DISCOUNT,
                        Location = line.LOCATION,
                        stockc.X_COLOUR,
                        SALESORD_Y_ONHOLD = order.ONHOLD == "Y",
                        NEW_DEF_LOCNO = accs.X_DEFLOC != line.LOCATION,
                        HDR_STATUS = order.STATUS
                    };

                var result = await salesOrderLines.ToListAsync();

                return result.Select(line =>
                {

                    bool defLocNo = line.NEW_DEF_LOCNO;
                    bool salesOrdOnHold = line.SALESORD_Y_ONHOLD;

                    return new SalesOrderLine
                    {
                        IsEditDisabled = line.Status > 19,
                        SOStatus = line.Status.GetValueOrDefault(),
                        SalesOrderId = line.HDR_SEQNO,
                        SalesOrderLineId = line.SEQNO,
                        Stockcode = line.STOCKCODE,
                        Description = line.DESCRIPTION,
                        Ordered = string.IsNullOrEmpty(line.X_ENTERED) ? (DateTime?)null : DateTime.Parse(line.X_ENTERED),
                        DelOn = line.DUEDATE,
                        Qty = line.UNSUP_QUANT,
                        Price = (decimal?)(line.UNITPRICE * (1 - (line.DISCOUNT / 100))),
                        Status = exo.Database.SqlQuery<string>("SELECT [dbo].[X_STOCK_AVAILABILITY_FN]({0}, {1})", line.STOCKCODE, line.Location).FirstOrDefault(),
                        StockCodeColour = line.X_COLOUR.Trim(),
                        HdrStatus = line.HDR_STATUS,
                        RowColour = (defLocNo && salesOrdOnHold) || (salesOrdOnHold && !defLocNo)
                        ? "#fffdc4"
                        : defLocNo ? "#facde1"
                        : "#ffffff"
                    };
                }).ToList();
            }
        }

        public async Task<SalesOrderLineDetails> GetSalesOrderLine(int salesOrderLineId, int status = 0)
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                var query =
                    from line in exo.SALESORD_LINES
                    join order in exo.SALESORD_HDR on line.HDR_SEQNO equals order.SEQNO
                    where line.SEQNO == salesOrderLineId
                    select new
                    {
                        line.SEQNO,
                        line.STOCKCODE,
                        SalesOrderId = order.SEQNO,
                        order.CUSTORDERNO,
                        Contact = exo.CONTACTS.FirstOrDefault(c => c.SEQNO == order.CONTACT_SEQNO),
                        order.ADDRESS1,
                        order.ADDRESS2,
                        order.ADDRESS3,
                        order.ADDRESS4,
                        order.ADDRESS5,
                        order.ADDRESS6,
                        line.DUEDATE,
                        line.UNSUP_QUANT,
                        StockItem = exo.STOCK_ITEMS.FirstOrDefault(s => s.STOCKCODE == line.STOCKCODE),
                        order.X_RunID,
                        line.LOCATION,
                        line.SOLINEID
                    };

                var result = await query.SingleOrDefaultAsync();

                var availabilityDate = await exo.Database.SqlQuery<DateTime>("SELECT [dbo].[X_STOCK_AVAILABILITY_DATES_FN]({0}, {1})", result.STOCKCODE, result.LOCATION).FirstOrDefaultAsync();

                var now = DateTime.Now;

                IQueryable<SALESORD_HDR> orders = status <= 0
                    ? exo.SALESORD_HDR.Where(order => order.STATUS == 0 || order.STATUS == 1)
                    : exo.SALESORD_HDR.Where(order => order.STATUS == status);

                var datesQuery = from p in exo.POSTCODES
                                 join or in orders on new { A = p.PLACE_POSTCODE, B = p.STATE, C = p.PLACE } equals new { A = or.ADDRESS6, B = or.ADDRESS5, C = or.ADDRESS4 }
                                 join rs in exo.X_RUN_SCHEDULE on p.X_RunID equals rs.RunID
                                 join ll in exo.SALESORD_LINES on or.SEQNO equals ll.HDR_SEQNO
                                 where
                                 rs.CutOff > now &&
                                 rs.CutOff > availabilityDate &&
                                 rs.DelDate.HasValue &&
                                 ll.SOLINEID == result.SOLINEID
                                 select rs.DelDate.Value;

                var validDelOnDates = await datesQuery.Distinct().OrderBy(d => d).ToArrayAsync();

                var orderLineStatus = await exo.Database.SqlQuery<string>("SELECT [dbo].[X_SO_LINE_CANCELLATION_FN]({0})", result.STOCKCODE).FirstOrDefaultAsync();

                return new SalesOrderLineDetails
                {
                    StockCode = result.STOCKCODE,
                    SalesOrderLineId = result.SEQNO,
                    SalesOrderId = result.SalesOrderId,
                    Address = $"{result.ADDRESS1 ?? ""} {result.ADDRESS2 ?? ""} {result.ADDRESS3 ?? ""} {result.ADDRESS4 ?? ""} {result.ADDRESS5 ?? ""} {result.ADDRESS6 ?? ""}",
                    DelOn = validDelOnDates.Any() && result.DUEDATE.HasValue && validDelOnDates.Min() > result.DUEDATE.Value ? (DateTime?)null : result.DUEDATE,
                    Qty = result.UNSUP_QUANT.HasValue ? (int)result.UNSUP_QUANT.Value : 0,
                    Reference = string.IsNullOrEmpty(result.CUSTORDERNO) && result.Contact != null ? $"{result.Contact.FIRSTNAME} {result.Contact.LASTNAME}" : result.CUSTORDERNO,
                    Status = orderLineStatus,
                    MinimumBuy = result.StockItem != null ? result.StockItem.X_MINIMUM_BUY ?? 0 : 0,
                    ValidDelOnDates = validDelOnDates
                };
            }
        }

        public async Task<List<PartnerListItem>> GetPartners(int customerAccountNumber)
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                var partners = await exo.X_PARTNER_ACCOUNT
                    .Where(x => x.ACCNO == customerAccountNumber)
                    .Select(x => new PartnerListItem
                    {
                        SeqNo = x.SEQNO,
                        PartnerName = x.X_PARTNER.NAME,
                        PartnerTypeName = x.X_PARTNER_TYPE.NAME
                    })
                    .AsNoTracking()
                    .ToListAsync();

                return partners;
            }
        }

        public async Task<IList<CustomerAccount>> GetCustomerAccounts(string email)
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                var contact = await exo.CONTACTS.AsNoTracking()
                    .FirstOrDefaultAsync(x => x.EMAIL == email && x.X_WEB == "Y" && x.ISACTIVE == "Y");

                if (contact == null)
                {
                    return null;
                }

                var query = from c in exo.DR_CONTACTS
                            join acc in exo.DR_ACCS on c.ACCNO equals acc.ACCNO
                            join la in exo.DR_ACCS_LAST_ACCESS on new { X1 = acc.ACCNO, X2 = contact.SEQNO } equals new { X1 = la.ACCNO, X2 = la.CONTACT_SEQNO } into x
                            from xx in x.DefaultIfEmpty()
                            where c.CONTACT_SEQNO == contact.SEQNO &&
                            c.ACCNO.HasValue &&
                            acc.ISACTIVE == "Y"
                            select new
                            {
                                acc.ACCNO,
                                acc.NAME,
                                acc.YEARVAL,
                                acc.LASTYEAR,
                                acc.X_RATING,
                                xx.LAST_ACCESS
                            };

                return await query.AsNoTracking().OrderByDescending(x => x.LAST_ACCESS).ThenByDescending(x => x.YEARVAL).ThenByDescending(x => x.LASTYEAR).ThenBy(x => x.X_RATING).ThenBy(x => x.NAME)
                    .Select(x => new CustomerAccount
                    {
                        AccountNumber = x.ACCNO,
                        AccountName = x.NAME
                    }).ToListAsync();
            }
        }

        public async Task<string> GetHubMessage(int id)
        {
            using (ExoEntities exo = exoContextFactory.CreateContext())
            {
                var message = await exo.X_HUB_Message.AsNoTracking()
                    .SingleOrDefaultAsync(x => x.SEQNO == id);

                if (message != null)
                {
                    return message.PHRASE;
                }

                return null;
            }
        }

        public async Task<List<EnquirySource>> GetEnquirySources()
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                var sources = await exo.X_ENQUIRY_SOURCE.AsNoTracking()
                    .Select(x => new EnquirySource
                    {
                        Id = x.SEQNO,
                        Source = x.SOURCE
                    }).ToListAsync();

                return sources;
            }
        }

        public async Task<bool> HavePastInvoicesToEdit()
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                var date = new DateTime(2021, 1, 1, 0, 0, 0, 0);

                var linesAvailableToEdit = await exo.DR_TRANS.AnyAsync(x =>
                    x.TRANSDATE >= date &&
                    x.TRANSTYPE == 1 &&
                    x.X_FREIGHT_REASON_DESC == null &&
                    (x.DISPATCH_INFO == null || exo.DISPMETHODs.Any(d => d.DESCRIPTION == x.DISPATCH_INFO && d.X_TRACK == "Y"))
                );

                return linesAvailableToEdit;
            }
        }

        public async Task<List<PastInvoice>> GetPastInvoices()
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                var date = new DateTime(2021, 1, 1, 0, 0, 0, 0);

                var query = from tran in exo.DR_TRANS
                            join acc in exo.DR_ACCS on tran.ACCNO equals acc.ACCNO
                            where tran.TRANSDATE >= date &&
                            tran.TRANSTYPE == 1 &&
                            tran.X_FREIGHT_REASON_DESC == null &&
                            (tran.DISPATCH_INFO == null || exo.DISPMETHODs.Any(d => d.DESCRIPTION == tran.DISPATCH_INFO && d.X_TRACK == "Y"))
                            select new
                            {
                                tran.SEQNO,
                                tran.TRANSDATE,
                                acc.NAME,
                                tran.INVNO,
                                tran.X_FREIGHT_REASON_DESC,
                                tran.POSTTIME,
                                tran.SALESNO
                            };

                var results = await query.AsNoTracking().OrderBy(x => x.POSTTIME).ToListAsync();

                return results.Select(r => new PastInvoice
                {
                    Id = r.SEQNO,
                    InvoiceDate = r.TRANSDATE,
                    Customer = r.NAME,
                    InvoiceNumber = r.INVNO,
                    Description = r.X_FREIGHT_REASON_DESC,
                    SalesNo = r.SALESNO
                }).ToList();
            }
        }

        public async Task<Dictionary<string, string>> GetFreightReasons()
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                var freightReasons = await exo.X_FREIGHT_REASON.AsNoTracking()
                    .ToDictionaryAsync(k => k.REASON, v => v.REASON);

                return freightReasons;
            }
        }

        public async Task<Dictionary<string, string>> GetContactRoles(string searchTerm)
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                var contactRoles = await exo.X_CONTACT_ROLE
                    .AsNoTracking()
                    .Where(r => r.ROLE.Contains(searchTerm))
                    .OrderBy(r => r.ROLE)
                    .ToDictionaryAsync(k => k.ROLE, v => v.ROLE);

                return contactRoles;
            }
        }

        public async Task<Dictionary<string, string>> GetAllContactRolesAsync()
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                var contactRoles = await exo.X_CONTACT_ROLE
                    .AsNoTracking()
                    .OrderBy(r => r.ROLE)
                    .ToDictionaryAsync(k => k.ROLE, v => v.ROLE);

                return contactRoles;
            }
        }

        public async Task<Dictionary<string, string>> GetInvoiceDeliveryTypes()
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                var invoiceDeliveryTypes = await exo.Database.SqlQuery<DictionaryResult>("SELECT [SEQNO] AS [Key], [DESCRIPTION] AS [Value] FROM [dbo].[X_AUTODOC_METHOD]")
                    .ToDictionaryAsync(k => k.Key.ToString(), v => v.Value);

                return invoiceDeliveryTypes;
            }
        }

        public async Task<Dictionary<int, string>> GetAccountGroups()
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                var accountGroups = await exo.DR_ACCGROUPS
                    .Where(x => x.ACCGROUP != 0)
                    .ToDictionaryAsync(k => k.ACCGROUP, v => v.GROUPNAME);

                return accountGroups;
            }
        }

        public async Task<List<DeliveryAddress>> GetSalesOrderDeliveryAddresses(int customerId)
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                var customer = await exo.DR_ACCS
                    .AsNoTracking()
                    .SingleAsync(acc => acc.ACCNO == customerId);

                var mainDeliveryAddress = new DeliveryAddress
                {
                    Identifier = customer.DELADDR1,
                    Address = customer.DELADDR2,
                    Additional = customer.DELADDR3,
                    Suburb = customer.DELADDR4,
                    State = customer.DELADDR5,
                    Postcode = customer.DELADDR6,
                };

                var deliveryAddresses = await exo.DR_ADDRESSES
                    .AsNoTracking()
                    .Where(addr => addr.ACCNO == customerId)
                    .Select(addr => new DeliveryAddress
                    {
                        Identifier = addr.DELADDR1,
                        Address = addr.DELADDR2,
                        Additional = addr.DELADDR3,
                        Suburb = addr.DELADDR4,
                        State = addr.DELADDR5,
                        Postcode = addr.DELADDR6,
                    })
                    .ToListAsync();

                deliveryAddresses.Insert(0, mainDeliveryAddress);

                var salesOrdersDeliveryAddresses = await exo.SALESORD_HDR
                    .AsNoTracking()
                    .Where(ord => ord.ACCNO == customer.ACCNO &&
                        ord.SALESORD_LINES.Any(line => line.UNSUP_QUANT > 0) &&
                        (ord.STATUS == 0 || ord.STATUS == 1))
                    .OrderByDescending(ord => ord.SEQNO)
                    .Select(ord => new DeliveryAddress
                    {
                        Identifier = ord.ADDRESS1,
                        Address = ord.ADDRESS2,
                        Additional = ord.ADDRESS3,
                        Suburb = ord.ADDRESS4,
                        State = ord.ADDRESS5,
                        Postcode = ord.ADDRESS6,
                        CustomerReference = ord.CUSTORDERNO
                    })
                    .ToListAsync();

                if (salesOrdersDeliveryAddresses.Count == 1)
                {
                    deliveryAddresses.Insert(0, salesOrdersDeliveryAddresses[0]);
                }
                else
                {
                    deliveryAddresses.AddRange(salesOrdersDeliveryAddresses);
                }

                // remove duplicates
                deliveryAddresses = deliveryAddresses.GroupBy(addr => addr.ToString()).Select(g => g.First()).ToList();

                deliveryAddresses.ForEach(addr => addr.Index = deliveryAddresses.IndexOf(addr));

                return deliveryAddresses;
            }
        }

        public async Task<GetStockStatusResult> GetStockStatus(string stockcode, string suburb, string state, string postcode, int? quantity)
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                var pc = await exo.POSTCODES
                    .AsNoTracking()
                    .Where(x => suburb == null || x.PLACE == suburb)
                    .Where(x => state == null || x.STATE == state)
                    .Where(x => postcode == null || x.PLACE_POSTCODE == postcode || x.BOX_POSTCODE == postcode)
                    .Select(x => new
                    {
                        x.SEQNO,
                        x.X_DEFLOC
                    })
                    .FirstOrDefaultAsync();

                if (pc != null && pc.X_DEFLOC.HasValue)
                {
                    var date = DateTime.Now;

                    var stockStatus = StockStatus.InStock;

                    var checkStockAvailability = quantity.HasValue && !string.IsNullOrEmpty(stockcode);

                    if (checkStockAvailability)
                    {
                        var stockAvail = await exo.X_free_stock_live
                            .AsNoTracking()
                            .Where(fsl => fsl.STOCKCODE == stockcode)
                            .ToArrayAsync();

                        var stockAvailForLocation = stockAvail.FirstOrDefault(fsl => fsl.Loc == pc.X_DEFLOC.Value);

                        if (stockAvailForLocation != null &&
                            quantity.Value > stockAvailForLocation.ShipNow)
                        {
                            if (stockAvailForLocation.ShipNow == 0)
                            {
                                stockStatus = StockStatus.NotInStock;

                                //var daysDelay = stockAvail.All(fsl => fsl.ShipNow == 0) ? 14 : 7;

                                //date = date.AddDays(daysDelay);
                            }
                            else
                            {
                                stockStatus = StockStatus.PartiallyInStock;
                            }
                        }

                    }

                    //var availabilityDate = date;

                    //if (!string.IsNullOrEmpty(stockcode))
                    //{
                    //    var stockDate = await exo.Database.SqlQuery<DateTime?>("SELECT [dbo].[X_STOCK_AVAILABILITY_DATES_FN]({0}, {1})", stockcode, pc.X_DEFLOC.Value).FirstOrDefaultAsync();

                    //    if (stockDate.HasValue)
                    //    {
                    //        availabilityDate = stockDate.Value;
                    //    }
                    //}

                    var datesQuery = from p in exo.POSTCODES
                                     join rs in exo.X_RUN_SCHEDULE on p.X_RunID equals rs.RunID
                                     where
                                     p.SEQNO == pc.SEQNO &&
                                     rs.CutOff > date &&
                                     //rs.CutOff > availabilityDate &&
                                     rs.DelDate.HasValue
                                     select rs.DelDate.Value;

                    var validDelOnDates = await datesQuery.Distinct().OrderBy(d => d).ToArrayAsync();

                    return new GetStockStatusResult
                    {
                        StockStatus = stockStatus.ToString(),
                        DeliveryDates = validDelOnDates
                    };
                }

                return null;
            }
        }

        public async Task<Dictionary<int, string>> GetOpenSalesOrderNumbers(int customerId, string address, string suburb, string state, string postcode)
        {
            return await GetOpenEntityNumbers(customerId, address, suburb, state, postcode, true);
        }

        public async Task<Dictionary<int, string>> GetOpenQuotesNumbers(int customerId, string address, string suburb, string state, string postcode)
        {
            return await GetOpenEntityNumbers(customerId, address, suburb, state, postcode, false);
        }

        public async Task<Dictionary<int, string>> GetOpenEntityNumbers(int customerId, string address, string suburb, string state, string postcode, bool isOrder = true)
        {
            const int lengthOrderNumberFormat = 10;

            using (var exo = exoContextFactory.CreateContext())
            {
                var orderQuery = from ord in exo.SALESORD_HDR
                                 join cnt in exo.CONTACTS on ord.CONTACT_SEQNO equals cnt.SEQNO
                                 where ord.ACCNO == customerId &&
                                        (!isOrder || (ord.X_MSP_AWMS_SO_ProgressNo <= 19 || ord.X_MSP_AWMS_SO_ProgressNo == null)) &&
                                        ord.SALESORD_LINES.Any(line => line.UNSUP_QUANT > 0) &&
                                        (isOrder
                                            ? (ord.STATUS == 0 || ord.STATUS == 1)
                                            : ord.STATUS == 3) &&
                                        ord.ADDRESS2.Contains(address) &&
                                        ord.ADDRESS4 == suburb &&
                                        ord.ADDRESS5 == state &&
                                        ord.ADDRESS6 == postcode
                                 select new
                                 {
                                     ord.SEQNO,
                                     Name = cnt.FIRSTNAME.Trim() + " " + cnt.LASTNAME.Trim()
                                 };

                Dictionary<int, string> orderNumbers = await orderQuery
                    .AsNoTracking()
                    .Distinct()
                    .OrderByDescending(o => o.SEQNO)
                    .ToDictionaryAsync(
                        k => k.SEQNO,
                        v => v.SEQNO.ToString().PadRight(lengthOrderNumberFormat) + v.Name);

                return orderNumbers;
            }
        }

        public async Task<bool> HasActiveCampaign(int customerId)
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                var accNo = await exo.Database.SqlQuery<int?>(@"SELECT X_Pipeline_Log.Accno
                    FROM X_Pipeline_Steps INNER JOIN X_Pipeline_Log ON X_Pipeline_Steps.Step = X_Pipeline_Log.Step
                    WHERE(LEN(X_Pipeline_Steps.Can_Become) <> 0) AND
                    (X_Pipeline_Log.Seqno IN(SELECT MAX(Seqno) AS Seqno FROM X_Pipeline_Log AS X_Pipeline_Log_1 GROUP BY Accno, Campaign))
                    AND X_Pipeline_Log.Accno = {0}", customerId).FirstOrDefaultAsync();

                return accNo.HasValue;
            }
        }

        public async Task<bool> HasTPBEnable(int customerId)
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                int accNo = await exo.Database.SqlQuery<int>("SELECT dbo.X_FN_TPB(@param)", new SqlParameter("@param", customerId)).FirstOrDefaultAsync();

                return (accNo == 1);
            }
        }

        public async Task<BestPriceResult> GetBestPrice(ExoEntities exo, string stockcode, int customerId, int qty, DateTime transactionDate)
        {
            var result = await exo.Database.SqlQuery<BestPriceResult>("[dbo].[BEST_PRICE] @STOCKCODE, @ACCNO, @QTY, @TRANSDATE, @BEST_FLAG",
                new SqlParameter("@STOCKCODE", stockcode),
                new SqlParameter("@ACCNO", customerId),
                new SqlParameter("@QTY ", qty),
                new SqlParameter("@TRANSDATE", transactionDate),
                new SqlParameter("@BEST_FLAG", "Y")).FirstOrDefaultAsync();

            result.SellPrice = await exo.STOCK_ITEMS
                .Where(s => s.STOCKCODE.Equals(stockcode))
                .Select(s => s.SELLPRICE1)
                .FirstOrDefaultAsync();

            return result;
        }

        public async Task<Dictionary<int, string>> GetOrderSources()
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                var orderSources = await exo.SALESORD_SOURCE
                    .AsNoTracking()
                    .ToDictionaryAsync(k => k.SEQNO, v => v.DESCRIPTION);

                return orderSources;
            }
        }

        public async Task<List<PastInvoice>> GetInvoicesByClaimAsync(int claimSeqNo)
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                var query = from claim in exo.X_CLAIMS
                            join acc in exo.DR_ACCS on claim.ACCNO equals acc.ACCNO
                            join tran in exo.DR_TRANS on acc.ACCNO equals tran.ACCNO
                            where claim.SEQNO == claimSeqNo
                            && tran.TRANSTYPE == 1
                            select new
                            {
                                tran.SEQNO,
                                tran.TRANSDATE,
                                tran.NAME,
                                tran.INVNO,
                                tran.POSTTIME,
                                tran.SALESNO
                            };

                var results = await query.AsNoTracking().OrderByDescending(x => x.POSTTIME).ToListAsync();

                return results.Select(r => new PastInvoice
                {
                    Id = r.SEQNO,
                    InvoiceDate = r.TRANSDATE,
                    InvoiceNumber = r.INVNO,
                    Description = r.NAME,
                    SalesNo = r.SALESNO
                }).ToList();
            }
        }

        public async Task<Dictionary<string, string>> GetInvoiceDescriptionsAsync(string invoiceNumber)
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                return await exo.DR_INVLINES
                     .AsNoTracking()
                     .Where(i => i.INVNO.Equals(invoiceNumber) && !i.STOCKCODE.Equals(InternalStockCode))
                     .ToDictionaryAsync(pair => pair.STOCKCODE, pair => pair.DESCRIPTION);
            }
        }

        public async Task<IEnumerable<BranchCusomer>> GetBranchesCustomerAsync(int accountNumber)
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                return await exo.DR_ACCS
                     .AsNoTracking()
                     .Where(i => i.HEAD_ACCNO.Equals(accountNumber))
                     .Select(acc => new BranchCusomer
                     {
                         AccNo = acc.ACCNO,
                         Name = acc.NAME,
                         Suburb = acc.DELADDR4,
                         State = acc.DELADDR5,
                         Postcode = acc.DELADDR6
                     })
                     .AsNoTracking()
                     .ToArrayAsync();
            }
        }

        public async Task<IEnumerable<ConsolidatedSalesOrderLine>> GetConsolidatedSalesOrderLinesAsync(int accountNumber)
        {
            var branches = new List<ConsolidatedSalesOrderLine>();
            using (var exo = exoContextFactory.CreateContext())
            {
                branches = await exo.DR_ACCS
                     .AsNoTracking()
                     .Where(i => i.HEAD_ACCNO.Equals(accountNumber))
                     .Select(acc => new ConsolidatedSalesOrderLine
                     {
                         AccNo = acc.ACCNO,
                         AccName = acc.NAME,

                     })
                     .AsNoTracking()
                     .ToListAsync();
            }

            var result = (await Task.WhenAll(branches.Select(async (value) => new
            {
                AccNo = value.AccNo,
                Name = value.AccName,
                Orders = await GetSalesOrderLines(value.AccNo).ConfigureAwait(false)
            })))
                .SelectMany(x => x.Orders,
                    (x, ord) => new ConsolidatedSalesOrderLine
                    {
                        AccNo = x.AccNo,
                        AccName = x.Name,
                        SalesOrderId = ord.SalesOrderId,
                        Stockcode = ord.Stockcode,
                        Description = ord.Description,
                        DelOn = ord.DelOn,
                        Qty = ord.Qty,
                        Ordered = ord.Ordered,
                        Price = ord.Price,
                        Status = ord.Status,
                        SalesOrderLineId = ord.SalesOrderLineId,
                        StockCodeColour = ord.StockCodeColour,
                        Address = ord.Address,
                        HdrStatus = ord.HdrStatus,
                        IsNewDefLocno = ord.IsNewDefLocno,
                        IsSaleSordOnHoldHaveY = ord.IsSaleSordOnHoldHaveY,
                        Reference = ord.Reference,
                        RowColour = ord.RowColour
                    })
                .ToArray();

            return result;
        }

        public async Task<IEnumerable<ConsolidatedCustomerTransaction>> GetConsolidatedTransactionsAsync(int loggedInStaffNo, int accountNumber)
        {
            var branches = new List<ConsolidatedCustomerTransaction>();
            using (var exo = exoContextFactory.CreateContext())
            {
                branches = await exo.DR_ACCS
                     .AsNoTracking()
                     .Where(i => i.HEAD_ACCNO.Equals(accountNumber))
                     .Select(acc => new ConsolidatedCustomerTransaction
                     {
                         AccNo = acc.ACCNO,
                         AccName = acc.NAME,

                     })
                     .AsNoTracking()
                     .ToListAsync();
            }

            var result = branches.Select(value => new
            {
                AccNo = value.AccNo,
                Name = value.AccName,
                Transactions = GetTransactions(loggedInStaffNo, value.AccNo, DateTime.UtcNow)
            })
                .SelectMany(x => x.Transactions,
                    (x, t) => new ConsolidatedCustomerTransaction
                    {
                        AccNo = x.AccNo,
                        AccName = x.Name,
                        Amount = t.Amount,
                        Date = t.Date,
                        DisplayTransaction = t.DisplayTransaction,
                        DueDate = t.DueDate,
                        Id = t.Id,
                        InvoiceNumber = t.InvoiceNumber,
                        Outstanding = t.Outstanding,
                        PaymentLink = t.PaymentLink,
                        Reference1 = t.Reference1,
                        Reference2 = t.Reference2,
                        Transaction = t.Transaction
                    })
                .ToArray();

            return result;
        }

        public async Task<Dictionary<int, string>> GetWebUserTypesAsync()
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                var types = await exo.X_WN_Usertype
                    .AsNoTracking()
                    .ToDictionaryAsync(k => k.UserType, v => v.Display);

                return types;
            }
        }

        public async Task<IList<BannedViewModel>> GetBannedItems(int accountNumber)
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                return await exo.X_BANNED
                    .AsNoTracking()
                    .Join(exo.STOCK_ITEMS, s => s.STOCKCODE, s => s.STOCKCODE, (b, s) => new { BannedItem = b, Description = s.DESCRIPTION })
                    .Where(b => b.BannedItem.ACCNO == accountNumber)
                    .Select(b => new BannedViewModel
                    {
                        SEQNO = b.BannedItem.SEQNO,
                        STOCKCODE = b.BannedItem.STOCKCODE,
                        ACCNO = b.BannedItem.ACCNO,
                        LIST_DATE = b.BannedItem.LIST_DATE,
                        COMMENT = b.BannedItem.COMMENT,
                        Description = b.Description
                    })
                    .ToListAsync();
            }
        }

        public async Task<X_BANNED> GetBannedItem(int seqNo)
        {
            using (var exo = exoContextFactory.CreateContext())
            {
                return await exo.X_BANNED
                    .AsNoTracking()
                    .FirstAsync(b => b.SEQNO == seqNo);
            }
        }
    }
}