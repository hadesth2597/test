using DevExpress.Xpo;
using log4net;
using PLAYTRAK.LocalDBModel.XPOModel.PLAYTRAK.LocalDBModel;
using PLAYTRAK.ReporteFidelizacion;
using PLAYTRAK.ReportesFidelizacion.Resources;
using PLAYTRAK.XPOHelper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using PLAYTRAK.DataFormat;
using PLAYTRAK.LocalDBModel.Models.Cliente;
using PLAYTRAK.LocalDBModel.Models.Extract;
using System.Web.WebPages;
using PLAYTRAK.LocalDBModel.Functions;
using System.Collections;
using System.Reflection.PortableExecutable;

namespace PLAYTRAK.ReportesFidelizacion.Models
{


    public class ClientsCategoryModel
    {
        const int RedeemPoints = 1050;
        const int DeliverTickets = 1070;
        /// <summary>
        /// Id client
        /// </summary>
        public int IdClient { get; set; }
        /// <summary>
        /// Name of client
        /// </summary>
        public string Client { get; set; }
        /// <summary>
        /// Date the user registered
        /// </summary>
        public DateTime? DateAdmission { get; set; }
        /// <summary>
        /// Date last visit client
        /// </summary>
        public DateTime? DateLastVisit { get; set; }
        /// <summary>
        /// Code Type Client
        /// </summary>
        public short CodTypeClient { get; set; }
        /// <summary>
        /// Type Client
        /// </summary>
        public string TypeClient { get; set; }
        /// <summary>
        /// Bet total client
        /// </summary>
        public decimal? TotalBet { get; set; }
        /// <summary>
        /// Email client
        /// </summary>
        public string Email { get; set; }
        /// <summary>
        /// Cel phone client
        /// </summary>
        public string Movil { get; set; }
        /// <summary>
        /// Phone client
        /// </summary>
        public string Phone { get; set; }
        /// <summary>
        /// Bet total client in range date
        /// </summary>
        public decimal? TotalPoints { get; set; }
        /// <summary>
        /// Date Billing
        /// </summary>
        public DateTime DateBilling { get; set; }
        /// <summary>
        /// Total points in days calculate
        /// </summary>
        public decimal? TotalPointsCalculate { get; set; }
        /// <summary>
        /// Time for session
        /// </summary>
        public TimeSpan TimeSesion { get; set; }
        /// <summary>
        /// Total time client in game
        /// </summary>
        public double TotalTimeSesion { get; set; }
        /// <summary>
        /// Win for client
        /// </summary>
        public decimal? TotalWin { get; set; }
        /// <summary>
        /// Jackpot for client
        /// </summary>
        public decimal? TotalJackpot { get; set; }
        /// <summary>
        /// TotalBet - (TotalWin + TotalJackpot)
        /// </summary>
        public decimal? TotalNetWin { get; set; }
        /// <summary>
        /// Clients
        /// </summary>
        public int TotalClient { get; set; }
        /// <summary>
        /// Category Color
        /// </summary>
        public string Color { get; set; }
        /// <summary>
        /// Visits
        /// </summary>
        public int Visits { get; set; }
        /// <summary>
        /// Business Line
        /// </summary>
        public int BusinessLineId { get; set; }
        /// <summary>
        /// Business Line Id
        /// </summary>
        public string BusinessLine { get; set; }
        /// <summary>
        /// Total points client
        /// </summary>
        public decimal ClientFidelPoints { get; set; }
        /// <summary>
        /// Points Redeemed in range time
        /// </summary>
        public decimal PointsRedeemed { get; set; }

        /// <summary>
        /// Method for retrieving information on users' profits, bets, playtime, etc., within a specified date range.
        /// </summary>
        /// <param name="idClient">Client identifier (optional).</param>
        /// <param name="startDate">Start date for the search.</param>
        /// <param name="endDate">End date for the search.</param>
        /// <param name="codCategory">String list separated by comma of category codes to retrieve (leave blank to retrieve all).</param>
        /// <param name="codTypeCredit">String list separated by comma of business line code to retrieve (e.g., Table, Machine, etc.). If left blank, all will be retrieved. Possible values are 'S' or 'M'.</param>
        /// <param name="onlyNewUser">Flag to indicate if only new users should be considered (default is false).</param>
        /// <returns>Response object containing the client category.</returns>
        public Response GetClientCategory(int? idClient, DateTime startDate, DateTime endDate, string codCategory, string codTypeCredit, bool onlyNewUser = false)
        {
            Response resp = new Response() { TypeOfResponse = TypeOfResponse.Ok };
            try
            {
                if (startDate == null || endDate == null)
                {
                    resp.TypeOfResponse = TypeOfResponse.Other;
                    resp.Message = ErrorMessages.DateRequired;
                    return resp;
                }

                if (startDate > endDate)
                {
                    resp.TypeOfResponse = TypeOfResponse.Other;
                    resp.Message = ErrorMessages.DateIsGreaterThan;
                    return resp;
                }

                List<string> listCodCategory = codCategory?.Split(',').ToList();
                List<string> listCodTypeCredit = codTypeCredit?.Split(',').ToList() ?? new List<string> { "S", "M" };
                List<char> listEstado = new List<char> { 'F', 'P', 'U', 'C' };
                using (UnitOfWork uow = XPOFunction.GetNewUnitOfWork(Logger.Logger.GetLog4netGlobal()))
                {

                    List<ClientsCategoryModel> listClientsCategory = new List<ClientsCategoryModel>();
                    List<ClientsCategoryModel> listClientsCategoryReturn;
                    var machineClientSessions =
                        (from fcsm in XPOFunction.XPQueryCatched<FIDEL_ClienteSesionMaquina>(uow, Logger.Logger.GetLog4netGlobal())
                         join fcd in XPOFunction.XPQueryCatched<FIDEL_CLIENTE_Datos>(uow, Logger.Logger.GetLog4netGlobal()) on fcsm.IDCliente.IDCliente equals fcd.IDCliente
                         join fcs in XPOFunction.XPQueryCatched<FIDEL_CLIENTE_Saldo>(uow, Logger.Logger.GetLog4netGlobal()) on fcd.IDCliente equals fcs.IDCliente
                         where fcsm.PartidasJugadas > 0 && listEstado.Contains(fcsm.Estado.CodEstado) && (idClient == null || fcsm.IDCliente.IDCliente == idClient)
                           && (fcsm.FechaFacturacion >= startDate && fcsm.FechaFacturacion <= endDate)
                           && (listCodCategory == null || listCodCategory.Contains(fcsm.IDCliente.CodTipoCliente.CodTipoCliente.ToString()))
                           && (listCodTypeCredit.Contains("S"))
                         orderby fcsm.FechaFacturacion descending
                         select new
                         {
                             IdClient = fcd.IDCliente,
                             DateBilling = fcsm.FechaFacturacion,
                             TotalTimeSesion = System.Data.Linq.SqlClient.SqlMethods.DateDiffMinute(fcsm.FechaHoraInicio, fcsm.FechaHoraFin.Value),
                             TotalBet = fcsm.Apostado,
                             TotalPoints = fcsm.PuntosFidelSumados,
                             TotalWin = fcsm.Ganado,
                             TotalJackpot = fcsm.Jackpots,
                             TotalNetWin = fcsm.Apostado - (fcsm.Ganado + fcsm.Jackpots),
                             BusinessLine = "Slot",
                             BusinessLineId = fcsm.IDMaquina.IDMaquina,
                             Visit = 1,
                             ClientTotalPoints = fcs.SaldoFidel,
                             Client = string.Format("{0} {1} {2}", fcd.ApellidoPaterno, fcd.ApellidoMaterno, fcd.Nombre),
                             Phone = fcd.TelefonoPrincipal,
                             Movil = fcd.TelefonoCelular,
                             Email = fcd.Email,
                             DateAdmission = fcd.FechaHoraAlta,
                             DateLastVisit = fcs.UltimaActualizacion,
                             TypeClient = fcd.CodTipoCliente.Descripcion,
                             CodTypeClient = fcd.CodTipoCliente.CodTipoCliente,
                             Color = fcd.CodTipoCliente.ColorIdentificador
                         }).ToList();
                    machineClientSessions = (
                             from mcs in machineClientSessions
                             group mcs by new
                             {
                                 mcs.IdClient,
                                 mcs.DateBilling
                             }
                         into item
                             select new
                             {
                                 IdClient = item.Key.IdClient,
                                 DateBilling = item.Key.DateBilling,
                                 TotalTimeSesion = item.Sum(x => x.TotalTimeSesion),
                                 TotalBet = item.Sum(x => x.TotalBet),
                                 TotalPoints = item.Sum(x => x.TotalPoints),
                                 TotalWin = item.Sum(x => x.TotalWin),
                                 TotalJackpot = item.Sum(x => x.TotalJackpot),
                                 TotalNetWin = item.Sum(x => x.TotalNetWin),
                                 BusinessLine = "Slot",
                                 BusinessLineId = item.Max(x => x.BusinessLineId),
                                 Visit = 1,
                                 ClientTotalPoints = item.FirstOrDefault().ClientTotalPoints,
                                 Client = item.FirstOrDefault().Client,
                                 Phone = item.FirstOrDefault().Phone,
                                 Movil = item.FirstOrDefault().Movil,
                                 Email = item.FirstOrDefault().Email,
                                 DateAdmission = item.FirstOrDefault().DateAdmission,
                                 DateLastVisit = item.FirstOrDefault().DateLastVisit,
                                 TypeClient = item.FirstOrDefault().TypeClient,
                                 CodTypeClient = item.FirstOrDefault().CodTypeClient,
                                 Color = item.FirstOrDefault().Color
                             }).ToList();

                    listClientsCategory.AddRange(
                              (
                               from mcs in machineClientSessions
                               group new { mcs } by new
                               {
                                   ClientTotalPoints = mcs.ClientTotalPoints,
                                   IdClient = mcs.IdClient,
                                   Client = mcs.Client,
                                   Phone = mcs.Phone,
                                   Movil = mcs.Movil,
                                   Email = mcs.Email,
                                   DateAdmission = mcs.DateAdmission,
                                   DateLastVisit = mcs.DateLastVisit,
                                   TypeClient = mcs.TypeClient,
                                   CodTypeClient = mcs.CodTypeClient,
                                   Color = mcs.Color,
                                   mcs.DateBilling,
                                   mcs.BusinessLineId
                               }
                               into groupClients
                               select new ClientsCategoryModel
                               {
                                   IdClient = groupClients.Key.IdClient,
                                   Client = groupClients.Key.Client,
                                   Phone = groupClients.Key.Phone,
                                   Movil = groupClients.Key.Movil,
                                   Email = groupClients.Key.Email,
                                   DateBilling = groupClients.Key.DateBilling,
                                   DateAdmission = groupClients.Key.DateAdmission,
                                   DateLastVisit = groupClients.Key.DateLastVisit,
                                   CodTypeClient = groupClients.Key.CodTypeClient,
                                   TypeClient = groupClients.Key.TypeClient,
                                   TotalTimeSesion = groupClients.Sum(x => x.mcs.TotalTimeSesion),
                                   TotalBet = groupClients.Sum(x => x.mcs.TotalBet),
                                   TotalPoints = groupClients.Sum(x => x.mcs.TotalPoints),
                                   TotalWin = groupClients.Sum(x => x.mcs.TotalWin),
                                   TotalJackpot = groupClients.Sum(x => x.mcs.TotalJackpot),
                                   TotalNetWin = groupClients.Sum(x => x.mcs.TotalNetWin),
                                   Color = groupClients.Key.Color,
                                   BusinessLine = "Slot",
                                   BusinessLineId = groupClients.Key.BusinessLineId,
                                   ClientFidelPoints = groupClients.Key.ClientTotalPoints,
                                   Visits = groupClients.Sum(x => x.mcs.Visit)
                               }
                              )
                           );

                        var tableClientSessions =
                             (from fcsm in XPOFunction.XPQueryCatched<FIDEL_ClienteSesionMesa>(uow, Logger.Logger.GetLog4netGlobal())
                              join fcd in XPOFunction.XPQueryCatched<FIDEL_CLIENTE_Datos>(uow, Logger.Logger.GetLog4netGlobal()) on fcsm.IDCliente.IDCliente equals fcd.IDCliente
                              join fcs in XPOFunction.XPQueryCatched<FIDEL_CLIENTE_Saldo>(uow, Logger.Logger.GetLog4netGlobal()) on fcd.IDCliente equals fcs.IDCliente
                              where fcsm.PartidasJugadas != null && listEstado.Contains(fcsm.Estado.CodEstado) && (idClient == null || fcsm.IDCliente.IDCliente == idClient)
                                && (fcsm.FechaFacturacion >= startDate && fcsm.FechaFacturacion <= endDate)
                                && (listCodCategory == null || listCodCategory.Contains(fcsm.IDCliente.CodTipoCliente.CodTipoCliente.ToString()))
                                && (listCodTypeCredit.Contains("M"))
                              orderby fcsm.FechaFacturacion descending
                              select new
                              {
                                  IdClient = fcd.IDCliente,
                                  DateBilling = fcsm.FechaFacturacion,
                                  TotalTimeSesion = System.Data.Linq.SqlClient.SqlMethods.DateDiffMinute(fcsm.FechaHoraInicio, fcsm.FechaHoraFin.Value),
                                  TotalBet = fcsm.Apostado,
                                  TotalPoints = fcsm.PuntosFidelSumados,
                                  TotalWin = fcsm.Ganado,
                                  TotalJackpot = fcsm.Jackpots,
                                  TotalNetWin = fcsm.Apostado - (fcsm.Ganado + fcsm.Jackpots),
                                  BusinessLine = "Table",
                                  BusinessLineId = fcsm.IDMesa.IDMesa,
                                  Visit = 1,
                                  ClientTotalPoints = fcs.SaldoFidel,
                                  Client = string.Format("{0} {1} {2}", fcd.ApellidoPaterno, fcd.ApellidoMaterno, fcd.Nombre),
                                  Phone = fcd.TelefonoPrincipal,
                                  Movil = fcd.TelefonoCelular,
                                  Email = fcd.Email,
                                  DateAdmission = fcd.FechaHoraAlta,
                                  DateLastVisit = fcs.UltimaActualizacion,
                                  TypeClient = fcd.CodTipoCliente.Descripcion,
                                  CodTypeClient = fcd.CodTipoCliente.CodTipoCliente,
                                  Color = fcd.CodTipoCliente.ColorIdentificador
                              }).OrderByDescending(o => o.DateBilling).ToList();
                        tableClientSessions = (
                                from mcs in tableClientSessions
                                group mcs by new
                                {
                                    mcs.IdClient,
                                    mcs.DateBilling
                                }
                            into item
                                select new
                                {
                                    IdClient = item.Key.IdClient,
                                    DateBilling = item.Key.DateBilling,
                                    TotalTimeSesion = item.Sum(x => x.TotalTimeSesion),
                                    TotalBet = item.Sum(x => x.TotalBet),
                                    TotalPoints = item.Sum(x => x.TotalPoints),
                                    TotalWin = item.Sum(x => x.TotalWin),
                                    TotalJackpot = item.Sum(x => x.TotalJackpot),
                                    TotalNetWin = item.Sum(x => x.TotalNetWin),
                                    BusinessLine = "Table",
                                    BusinessLineId = item.FirstOrDefault().BusinessLineId,
                                    Visit = 1,
                                    ClientTotalPoints = item.FirstOrDefault().ClientTotalPoints,
                                    Client = item.FirstOrDefault().Client,
                                    Phone = item.FirstOrDefault().Phone,
                                    Movil = item.FirstOrDefault().Movil,
                                    Email = item.FirstOrDefault().Email,
                                    DateAdmission = item.FirstOrDefault().DateAdmission,
                                    DateLastVisit = item.FirstOrDefault().DateLastVisit,
                                    TypeClient = item.FirstOrDefault().TypeClient,
                                    CodTypeClient = item.FirstOrDefault().CodTypeClient,
                                    Color = item.FirstOrDefault().Color
                                }).OrderByDescending(o => o.DateBilling).ToList();
                        listClientsCategory.AddRange(
                                     (
                                      from mcs in tableClientSessions
                                      group new { mcs } by new
                                      {
                                          ClientTotalPoints = mcs.ClientTotalPoints,
                                          IdClient = mcs.IdClient,
                                          Client = mcs.Client,
                                          Phone = mcs.Phone,
                                          Movil = mcs.Movil,
                                          Email = mcs.Email,
                                          DateAdmission = mcs.DateAdmission,
                                          DateLastVisit = mcs.DateLastVisit,
                                          TypeClient = mcs.TypeClient,
                                          CodTypeClient = mcs.CodTypeClient,
                                          Color = mcs.Color,
                                          mcs.DateBilling,
                                          mcs.BusinessLineId
                                      }
                                      into groupClients
                                      select new ClientsCategoryModel
                                      {
                                          IdClient = groupClients.Key.IdClient,
                                          Client = groupClients.Key.Client,
                                          Phone = groupClients.Key.Phone,
                                          Movil = groupClients.Key.Movil,
                                          Email = groupClients.Key.Email,
                                          DateBilling = groupClients.Key.DateBilling,
                                          DateAdmission = groupClients.Key.DateAdmission,
                                          DateLastVisit = groupClients.Key.DateLastVisit,
                                          CodTypeClient = groupClients.Key.CodTypeClient,
                                          TypeClient = groupClients.Key.TypeClient,
                                          TotalTimeSesion = groupClients.Sum(x => x.mcs.TotalTimeSesion),
                                          TotalBet = groupClients.Sum(x => x.mcs.TotalBet),
                                          TotalPoints = groupClients.Sum(x => x.mcs.TotalPoints),
                                          TotalWin = groupClients.Sum(x => x.mcs.TotalWin),
                                          TotalJackpot = groupClients.Sum(x => x.mcs.TotalJackpot),
                                          TotalNetWin = groupClients.Sum(x => x.mcs.TotalNetWin),
                                          Color = groupClients.Key.Color,
                                          BusinessLine = "Table",
                                          BusinessLineId = groupClients.Key.BusinessLineId,
                                          ClientFidelPoints = groupClients.Key.ClientTotalPoints,
                                          Visits = groupClients.Sum(x => x.mcs.Visit)
                                      }
                                     )
                                  );
                    listClientsCategoryReturn = listClientsCategory
                .Where(lcc => lcc.DateBilling >= startDate && lcc.DateBilling <= endDate)
                .GroupBy(group => new
                {
                    group.IdClient,
                    group.Client,
                    group.Phone,
                    group.Movil,
                    group.Email,
                    group.DateAdmission,
                    group.DateLastVisit,
                    group.CodTypeClient,
                    group.TypeClient,
                    group.TotalPointsCalculate,
                    group.ClientFidelPoints

                })
                .Select(item => new ClientsCategoryModel()
                {
                    IdClient = item.Key.IdClient,
                    Client = item.Key.Client,
                    Phone = item.Key.Phone,
                    Movil = item.Key.Movil,
                    Email = item.Key.Email,
                    DateAdmission = item.Key.DateAdmission,
                    DateLastVisit = item.Key.DateLastVisit,
                    CodTypeClient = item.Key.CodTypeClient,
                    TypeClient = item.Key.TypeClient,
                    TotalTimeSesion = item.Sum(x => x.TotalTimeSesion),
                    TotalBet = item.Sum(x => x.TotalBet),
                    TotalPoints = item.Sum(x => x.TotalPoints),
                    TotalWin = item.Sum(x => x.TotalWin),
                    TotalJackpot = item.Sum(x => x.TotalJackpot),
                    TotalNetWin = item.Sum(x => x.TotalNetWin),
                    Color = item.FirstOrDefault().Color,
                    BusinessLine = item.FirstOrDefault().BusinessLine,
                    BusinessLineId = item.FirstOrDefault().BusinessLineId,
                    Visits = item.Sum(x => x.Visits),
                    ClientFidelPoints = item.Key.ClientFidelPoints
                }).ToList();

                    object[] result = new object[2];

                    var dataReturn = (from fo in XPOFunction.XPQueryCatched<FIDEL_Operacion>(uow, Logger.Logger.GetLog4netGlobal())
                                      join fcd in XPOFunction.XPQueryCatched<FIDEL_CLIENTE_Datos>(uow, Logger.Logger.GetLog4netGlobal()) on fo.IDCliente.IDCliente equals fcd.IDCliente
                                      join fcs in XPOFunction.XPQueryCatched<FIDEL_CLIENTE_Saldo>(uow, Logger.Logger.GetLog4netGlobal()) on fcd.IDCliente equals fcs.IDCliente
                                      where (fo.CodTipoOperacion.CodTipoOperacion == RedeemPoints || fo.CodTipoOperacion.CodTipoOperacion == DeliverTickets) && (idClient == null ||
                                      fo.IDCliente.IDCliente == idClient) && (fo.FechaFacturacion >= startDate && fo.FechaFacturacion <= endDate)
                                      select new
                                      {
                                          IdClient = fo.IDCliente.IDCliente,
                                          PointsRedeemed = (fo.ImportePuntos == null ? 0 : fo.ImportePuntos ?? 0),
                                          ClientTotalPoints = fcs.SaldoFidel,
                                          fcd.ApellidoPaterno,
                                          fcd.ApellidoMaterno,
                                          fcd.Nombre,
                                          fcd.TelefonoPrincipal,
                                          fcd.TelefonoCelular,
                                          fcd.Email,
                                          fcd.FechaHoraAlta,
                                          fcs.UltimaActualizacion,
                                          fcd.CodTipoCliente.Descripcion,
                                          fcd.CodTipoCliente.CodTipoCliente,
                                          fcd.CodTipoCliente.ColorIdentificador,
                                          fo.FechaFacturacion
                                      }).OrderByDescending(o => o.FechaFacturacion).ToList();
                    var listPointReemed =
                                       (
                                        from mcs in dataReturn
                                        group mcs by new
                                        {
                                            IdClient = mcs.IdClient,
                                            ClientTotalPoints = mcs.ClientTotalPoints,
                                            Client = string.Format("{0} {1} {2}", mcs.ApellidoPaterno, mcs.ApellidoMaterno, mcs.Nombre),
                                            Phone = mcs.TelefonoPrincipal,
                                            Movil = mcs.TelefonoCelular,
                                            Email = mcs.Email,
                                            DateAdmission = mcs.FechaHoraAlta,
                                            DateLastVisit = mcs.UltimaActualizacion,
                                            TypeClient = mcs.Descripcion,
                                            CodTypeClient = mcs.CodTipoCliente,
                                            Color = mcs.ColorIdentificador,
                                            DateBilling = mcs.FechaFacturacion,
                                            BusinessLineId = 0,
                                            mcs.PointsRedeemed
                                        }
                                        into groupClients
                                        select new ClientsCategoryModel
                                        {
                                            IdClient = groupClients.Key.IdClient,
                                            Client = groupClients.Key.Client,
                                            Phone = groupClients.Key.Phone,
                                            Movil = groupClients.Key.Movil,
                                            Email = groupClients.Key.Email,
                                            DateBilling = groupClients.Key.DateBilling,
                                            DateAdmission = groupClients.Key.DateAdmission,
                                            DateLastVisit = groupClients.Key.DateLastVisit,
                                            CodTypeClient = groupClients.Key.CodTypeClient,
                                            TypeClient = groupClients.Key.TypeClient,
                                            TotalTimeSesion = 0,
                                            TotalBet = 0,
                                            TotalPoints = 0,
                                            TotalWin = 0,
                                            TotalJackpot = 0,
                                            TotalNetWin = 0,
                                            Color = groupClients.Key.Color,
                                            BusinessLine = "Canje",
                                            BusinessLineId = 0,
                                            ClientFidelPoints = groupClients.Key.ClientTotalPoints,
                                            PointsRedeemed = groupClients.Key.PointsRedeemed
                                        }
                                    );
                    listClientsCategoryReturn.AddRange(listPointReemed);

                    listClientsCategoryReturn = listClientsCategoryReturn.GroupBy(group => new
                    {
                        group.IdClient,
                    })
                            .Select(item => new ClientsCategoryModel()
                            {
                                IdClient = item.Key.IdClient,
                                Client = item.FirstOrDefault().Client,
                                Phone = item.FirstOrDefault().Phone,
                                Movil = item.FirstOrDefault().Movil,
                                Email = item.FirstOrDefault().Email,
                                DateAdmission = item.FirstOrDefault().DateAdmission,
                                DateLastVisit = item.FirstOrDefault().DateLastVisit,
                                CodTypeClient = item.FirstOrDefault().CodTypeClient,
                                TypeClient = item.FirstOrDefault().TypeClient,
                                TotalTimeSesion = item.Sum(x => x.TotalTimeSesion),
                                TotalBet = item.Sum(x => x.TotalBet),
                                TotalPoints = item.Sum(x => x.TotalPoints),
                                TotalWin = item.Sum(x => x.TotalWin),
                                TotalJackpot = item.Sum(x => x.TotalJackpot),
                                TotalNetWin = item.Sum(x => x.TotalNetWin),
                                Color = item.FirstOrDefault().Color,
                                BusinessLine = item.FirstOrDefault().BusinessLine,
                                BusinessLineId = item.FirstOrDefault().BusinessLineId,
                                Visits = item.Sum(x => x.Visits),
                                ClientFidelPoints = item.Max(x => x.ClientFidelPoints),
                                PointsRedeemed = item.Sum(x => x.PointsRedeemed),
                            })
                            .ToList();

                    if (onlyNewUser)
                    {
                        listClientsCategoryReturn = listClientsCategoryReturn.Where(x => x.DateAdmission >= startDate && x.DateAdmission <= endDate).ToList();

                    }
                    result[0] = listClientsCategoryReturn;
                    result[1] = GetTotalClientCategory("A").Data;
                    resp.Data = result;
                    resp.TypeOfResponse = TypeOfResponse.Ok;
                }
            }
            catch (Exception ex)
            {
                resp.TypeOfResponse = TypeOfResponse.Error;
                resp.Message = "An error occurred while executing the GetClientCategory method";
                Logger.Logger.GetLog4netGlobal().ErrorFormat("An error occurred while executing the method: {0}. Exception: {1}", "GetClientCategory", ex);
            }
            return resp;
        }

        /// <summary>
        /// Method to get the total for a specific category based on the provided state code.
        /// </summary>
        /// <param name="codEstado">Code representing the state for which the category total is requested.</param>
        /// <returns>Response object containing the total for the specified category.</returns>
        public Response GetTotalClientCategory(string codEstado)
        {
            string MethodName = MethodBase.GetCurrentMethod().Name;
            string message = String.Format(PlayTrak.StartExecuteMethod, MethodName);
            Logger.Logger.GetLog4netGlobal().Info(message);
            Response resp = new Response() { TypeOfResponse = TypeOfResponse.Ok };
            try
            {
                List<string> listCodEstado = null;
                if (!string.IsNullOrEmpty(codEstado))
                {
                    listCodEstado = codEstado.Split(',').ToList();
                }
                using (UnitOfWork uow = XPOFunction.GetNewUnitOfWork(Logger.Logger.GetLog4netGlobal()))
                {
                    List<TotalClientsCategory> categorys = new List<TotalClientsCategory>();
                    categorys.Add(new TotalClientsCategory()
                    {
                        CodCategory = -1,
                        Category = "Todas",
                        Active = true,
                        TotalClient = 0
                    });
                    categorys.AddRange(XPOFunction.XPQueryCatched<FIDEL_CLIENTE_Datos>(uow, Logger.Logger.GetLog4netGlobal()).Where(x => (listCodEstado == null || listCodEstado.Contains(x.Estado.CodEstado.ToString()))).
                        GroupBy(group => new
                        {
                            group.CodTipoCliente.CodTipoCliente,
                            group.CodTipoCliente.Activo,
                            group.CodTipoCliente.Descripcion

                        })
                    .Select(item => new TotalClientsCategory()
                    {
                        CodCategory = item.Key.CodTipoCliente,
                        Category = item.Key.Descripcion,
                        TotalClient = item.Count(),
                        Active = item.Key.Activo
                    })
                    .ToList());
                    categorys.FirstOrDefault().TotalClient = categorys.Sum(x => x.TotalClient);
                    resp.Data = categorys;
                    resp.TypeOfResponse = TypeOfResponse.Ok;
                }
            }
            catch (Exception ex)
            {
                resp.TypeOfResponse = TypeOfResponse.Error;
                resp.Message = ex.Message;
                message = string.Format(PlayTrak.ErrorExecuteMethod, MethodName);
                Logger.Logger.GetLog4netGlobal().Error(message, ex);
            }
            return resp;
        }

        /// <summary>
        /// Method to extract sessions from machines and take them to the cloud.
        /// </summary>
        /// <param name="extract">class that receives parameters for extraction.</param>
        /// <returns>List from type model ExtractedCategoryModel</returns>
        public LocalDBModel.Models.Response ExtractMachines(ExtractParameters extract)
        {
            Logger.Logger.GetLog4netGlobal().Info(PlayTrak.StartExecute);
            LocalDBModel.Models.Response resp = new LocalDBModel.Models.Response { Message = PlayTrak.SuccessExecute, TypeOfResponse = LocalDBModel.Models.TypeOfResponse.Ok };
            var businessLine = "Slot";
            using (UnitOfWork uow = XPOFunction.GetNewUnitOfWork(Logger.Logger.GetLog4netGlobal()))
            {
                try
                {
                    if (extract.rowVersion == null)
                    {
                        if (ValidateDates.ValidateMonth(extract.startDate, extract.endDate, out resp, Logger.Logger.GetLog4netGlobal()))
                        {
                            resp.Data = new Extracted { Categories = ExtractSessions(extract, businessLine), LastRowVersion = null };
                        }
                    }
                    else
                    {
                        if (ValidateDates.ValidateForRowVersion(extract.startDate, extract.endDate, extract.rowVersion, out resp, Logger.Logger.GetLog4netGlobal()))
                        {
                            var result = ExtractSessions(extract, businessLine);
                            resp.Data = new Extracted { Categories = result, LastRowVersion = result.Count > 0 ? result.OrderByDescending(x => x.rowVersionLong).FirstOrDefault().rowVersion : null };
                        }
                    }
                }
                catch (Exception ex)
                {
                    resp.TypeOfResponse = LocalDBModel.Models.TypeOfResponse.Error;
                    resp.Message = PlayTrak.ErrorExecute;
                    Logger.Logger.GetLog4netGlobal().Error("An exception occurred while executing the ExtractMachines method", ex);
                }
            }
            return resp;
        }

        /// <summary>
        /// Method to extract sessions from tables and take them to the cloud.
        /// </summary>
        /// <param name="extract">class that receives parameters for extraction.</param>
        /// <returns>List from type model ExtractedCategoryModel</returns>
        public LocalDBModel.Models.Response ExtractTables(ExtractParameters extract)
        {
            Logger.Logger.GetLog4netGlobal().Info(PlayTrak.StartExecute);
            LocalDBModel.Models.Response resp = new LocalDBModel.Models.Response { Message = PlayTrak.SuccessExecute, TypeOfResponse = LocalDBModel.Models.TypeOfResponse.Ok };
            var businessLine = "Table";
            using (UnitOfWork uow = XPOFunction.GetNewUnitOfWork(Logger.Logger.GetLog4netGlobal()))
            {
                try
                {
                    if (extract.rowVersion == null)
                    {
                        if (ValidateDates.ValidateMonth(extract.startDate, extract.endDate, out resp, Logger.Logger.GetLog4netGlobal()))
                        {
                            resp.Data = new Extracted { Categories = ExtractSessions(extract, businessLine), LastRowVersion = null };
                        }
                    }
                    else
                    {
                        if (ValidateDates.ValidateForRowVersion(extract.startDate, extract.endDate, extract.rowVersion, out resp, Logger.Logger.GetLog4netGlobal()))
                        {
                            var result = ExtractSessions(extract, businessLine);
                            resp.Data = new Extracted { Categories = result, LastRowVersion = result.Count > 0 ? result.OrderByDescending(x => x.rowVersionLong).FirstOrDefault().rowVersion : null };
                        }
                    }
                }
                catch (Exception ex)
                {
                    resp.TypeOfResponse = LocalDBModel.Models.TypeOfResponse.Error;
                    resp.Message = PlayTrak.ErrorExecute;
                    Logger.Logger.GetLog4netGlobal().Error(BusinessLines.ErrorMessageExtract, ex);
                }
            }
            return resp;
        }

        /// <summary>
        /// Method to extract the clients table session or machines session.
        /// </summary>
        /// <param name="extract">class that receives parameters for extraction.</param>
        /// <param name="businessLine">businessLine</param>
        /// <returns>List from type model ExtractedCategoryModel</returns>
        private List<ExtractedCategoryModel> ExtractSessions(ExtractParameters extract, string businessLine)
        {
            Logger.Logger.GetLog4netGlobal().Info(PlayTrak.StartExecute);
            Response resp = new Response() { TypeOfResponse = TypeOfResponse.Ok };
            List<ExtractedCategoryModel> listClientsCategoryReturn = new List<ExtractedCategoryModel>();
            var result = new Pagination(extract.pageSize);
            extract.pageNumber = extract.pageNumber == 0 || extract.pageNumber == null ? 1 : extract.pageNumber;
            var dataResult = GetPointsRedeemed(extract.startDate, extract.endDate, extract.pageNumber, result.pageSize);
            try
            {
                using (UnitOfWork uow = XPOFunction.GetNewUnitOfWork(Logger.Logger.GetLog4netGlobal()))
                {
                    switch (businessLine)
                    {
                        case "Slot":
                            var machineSessionsSummary = GetData(extract).ToList();
                            var finalSessionsDataList = CreateListData(machineSessionsSummary);
                            listClientsCategoryReturn = GetExtactedCategoryModel(finalSessionsDataList);
                            break;
                        case "Table":
                            listClientsCategoryReturn = ExtractTableSessions(extract, businessLine);
                            break;
                    }
                    listClientsCategoryReturn.ForEach(x => x.pointsRedeemed = dataResult.Where(y => y.IdClient == x.idClient).Select(y => y.PointsRedeemed).FirstOrDefault());
                }
            }
            catch (Exception ex)
            {
                resp.TypeOfResponse = TypeOfResponse.Error;
                resp.Message = PlayTrak.ErrorExecute;
                Logger.Logger.GetLog4netGlobal().Error("Ocurrió una excepción al ejecutar el método ExtractedMachinesOrTables", ex);
            }
            return listClientsCategoryReturn;
        }

        /// <summary>
        /// Method to extract the clients table sessions.
        /// </summary>
        /// <param name="extract">class that receives parameters for extraction.</param>
        /// <param name="businessLine">businessLine</param>
        /// <returns>List from type model ExtractedCategoryModel</returns>
        private List<ExtractedCategoryModel> ExtractTableSessions(ExtractParameters extract, string businessLine)
        {
            Logger.Logger.GetLog4netGlobal().Info(PlayTrak.StartExecute);
            Response resp = new Response() { TypeOfResponse = TypeOfResponse.Ok };
            List<ExtractedCategoryModel> listClientsCategoryReturn = new List<ExtractedCategoryModel>();
            List<char> typeState = new List<char>() { 'F', 'P', 'U', 'C' };
            var result = new Pagination(extract.pageSize);
            extract.pageNumber = extract.pageNumber == 0 || extract.pageNumber == null ? 1 : extract.pageNumber;
            try
            {
                using (UnitOfWork uow = XPOFunction.GetNewUnitOfWork(Logger.Logger.GetLog4netGlobal()))
                {
                    var query = (from fcsm in XPOFunction.XPQueryCatched<FIDEL_ClienteSesionMesa>(uow, x => (extract.rowVersion == null || x.RowVersion.Compare(extract.rowVersion) > 0) && x.PartidasJugadas != null && typeState.Contains(x.Estado.CodEstado)
                                 && (extract.startDate == null || x.FechaFacturacion >= extract.startDate) && (extract.endDate == null || x.FechaFacturacion <= extract.endDate), Logger.Logger.GetLog4netGlobal())
                                 join md in XPOFunction.XPQueryCatched<MESA_Detalle>(uow, Logger.Logger.GetLog4netGlobal()) on fcsm.IDMesa.IDMesa equals md.IDMesa into r
                                 from md in r.DefaultIfEmpty()
                                 join fcd in XPOFunction.XPQueryCatched<FIDEL_CLIENTE_Datos>(uow, Logger.Logger.GetLog4netGlobal()) on fcsm.IDCliente.IDCliente equals fcd.IDCliente
                                 join fcs in XPOFunction.XPQueryCatched<FIDEL_CLIENTE_Saldo>(uow, Logger.Logger.GetLog4netGlobal()) on fcd.IDCliente equals fcs.IDCliente
                                 join fcdet in XPOFunction.XPQueryCatched<FIDEL_CLIENTE_Detalles>(uow, Logger.Logger.GetLog4netGlobal()) on fcd.IDCliente equals fcdet.IDCliente
                                 orderby fcsm.NroSesion
                                 select new
                                 {
                                     idClient = fcsm.IDCliente.IDCliente,
                                     client = string.Format("{0} {1} {2}", fcd.Nombre, fcd.ApellidoPaterno, fcd.ApellidoMaterno),
                                     gender = fcd.Sexo.IDSexo,
                                     age = DateFunction.GetAge((DateTime)fcd.FechaNacimiento),
                                     birthDate = (DateTime)fcd.FechaNacimiento,
                                     email = fcd.Email,
                                     observations = fcdet.Observaciones,
                                     cellPhone = fcd.TelefonoCelular,
                                     billingDate = (DateTime)fcd.FechaHoraAlta,
                                     businessLine = businessLine,
                                     accountingDate = fcsm.FechaFacturacion,
                                     typeClient = fcd.CodTipoCliente.Descripcion,
                                     clientFidelPoints = fcs.SaldoFidel,
                                     totalBet = fcsm.Apostado,
                                     totalWin = fcsm.Ganado,
                                     totalJackpot = fcsm.Jackpots,
                                     totalNetWin = fcsm.Apostado - (fcsm.Ganado + fcsm.Jackpots),
                                     hold = 0,
                                     teorico = md.PartidasHoraTeorico,
                                     gamesPlayed = fcsm.PartidasJugadas,
                                     gamesWon = 0,
                                     gamesLost = 0,
                                     totalPoints = fcsm.PuntosFidelSumados,
                                     pointsRedeemed = 0,
                                     totalTimeSesion = System.Data.Linq.SqlClient.SqlMethods.DateDiffMinute(fcsm.FechaHoraInicio, fcsm.FechaHoraFin.Value),
                                     rowVersion = fcsm.RowVersion,
                                     nroSesion = fcsm.NroSesion,
                                     rowVersionLong = LongFunction.GetRowversion(fcsm.RowVersion)
                                 }).Skip(((int)extract.pageNumber - 1) * (int)result.pageSize).Take((int)result.pageSize).ToList();
                    listClientsCategoryReturn = query.GroupBy(x => new
                    {
                        x.idClient,
                        x.accountingDate
                    }).Select(item => new ExtractedCategoryModel
                    {
                        idClient = item.Key.idClient,
                        client = item.FirstOrDefault().client,
                        gender = item.FirstOrDefault().gender,
                        age = item.FirstOrDefault().age,
                        birthDate = item.FirstOrDefault().birthDate,
                        email = item.FirstOrDefault().email,
                        observations = item.FirstOrDefault().observations,
                        cellPhone = item.FirstOrDefault().cellPhone,
                        billingDate = item.FirstOrDefault().billingDate,
                        businessLine = item.FirstOrDefault().businessLine,
                        accountingDate = item.FirstOrDefault().accountingDate,
                        typeClient = item.FirstOrDefault().typeClient,
                        clientFidelPoints = item.Sum(x => x.clientFidelPoints),
                        totalBet = item.Sum(x => x.totalBet),
                        totalWin = item.Sum(x => x.totalWin),
                        totalJackpot = item.Sum(x => x.totalJackpot),
                        totalNetWin = item.Sum(x => x.totalNetWin),
                        hold = DecimalFunction.CalculateHold(item.Sum(x => (decimal)x.totalBet), item.Sum(x => (decimal)x.totalWin), item.Sum(x => (decimal)x.totalJackpot)),
                        gamesPlayed = item.Sum(x => x.gamesPlayed),
                        gamesWon = item.Sum(x => x.gamesWon),
                        gamesLost = item.Sum(x => x.gamesLost),
                        totalPoints = item.Sum(x => x.totalPoints),
                        pointsRedeemed = 0,
                        totalTimeSesion = item.Sum(x => x.totalTimeSesion),
                        rowVersion = item.FirstOrDefault().rowVersion,
                        rowVersionLong = item.FirstOrDefault().rowVersionLong
                    }).ToList();
                }
            }
            catch (Exception ex)
            {
                resp.TypeOfResponse = TypeOfResponse.Error;
                resp.Message = PlayTrak.ErrorExecute;
                Logger.Logger.GetLog4netGlobal().Error("Ocurrió una excepción al ejecutar el método ExtractTableSessions", ex);
            }
            return listClientsCategoryReturn;
        }

        /// <summary>
        /// Method to obtain the points redeemed.
        /// </summary>
        /// <param name="startDate">Start date for the search.</param>
        /// <param name="endDate">End date for the search.</param>
        /// <param name="pageNumber">Page number for the search.</param>
        /// <param name="pageSize">Page size for the search</param>
        /// <returns>Generic list</returns>
        private List<(int IdClient, decimal PointsRedeemed)> GetPointsRedeemed(DateTime? startDate, DateTime? endDate, int? pageNumber, int? pageSize)
        {
            using (UnitOfWork uow = XPOFunction.GetNewUnitOfWork(Logger.Logger.GetLog4netGlobal()))
            {
                if (startDate != null && endDate != null)
                {
                    var dataReturn = (from fo in XPOFunction.XPQueryCatched<FIDEL_Operacion>(uow, Logger.Logger.GetLog4netGlobal())
                                      join fcd in XPOFunction.XPQueryCatched<FIDEL_CLIENTE_Datos>(uow, Logger.Logger.GetLog4netGlobal()) on fo.IDCliente.IDCliente equals fcd.IDCliente
                                      join fcs in XPOFunction.XPQueryCatched<FIDEL_CLIENTE_Saldo>(uow, Logger.Logger.GetLog4netGlobal()) on fcd.IDCliente equals fcs.IDCliente
                                      where (fo.CodTipoOperacion.CodTipoOperacion == RedeemPoints || fo.CodTipoOperacion.CodTipoOperacion == DeliverTickets) && (fo.FechaFacturacion >= startDate && fo.FechaFacturacion <= endDate)
                                      select new
                                      {
                                          IdClient = fo.IDCliente.IDCliente,
                                          PointsRedeemed = (fo.ImportePuntos == null ? 0 : fo.ImportePuntos ?? 0),
                                      }).OrderByDescending(x => x.IdClient).Skip(((int)pageNumber - 1) * (int)pageSize).Take((int)pageSize).ToList();
                    var clientPointsSummary = dataReturn.GroupBy(x => new { x.IdClient }).Select(y => new { idClient = y.Key.IdClient, PointsRedeemed = y.Sum(x => x.PointsRedeemed) }).ToList();
                    return clientPointsSummary.Select(x => (x.idClient, x.PointsRedeemed)).ToList();
                }
                else
                {
                    var dataResult = new List<object>().Select(x => new { IdClient = default(int), PointsRedeemed = default(decimal) }).ToList();
                    return dataResult.Select(x => (x.IdClient, x.PointsRedeemed)).ToList();
                }
            }
        }

        /// <summary>
        /// Method to obtain machine sessions.
        /// </summary>
        /// <param name="extract">class that receives parameters for extraction.</param>
        /// <returns>Generic list to machine sessions.</returns>
        private List<ExtractedCategoryModel> GetData(ExtractParameters extract)
        {
            List<char> typeState = new List<char>() { 'F', 'P', 'U', 'C' };
            var result = new Pagination(extract.pageSize);
            var businessLine = "Slot";
            using (UnitOfWork uow = XPOFunction.GetNewUnitOfWork(Logger.Logger.GetLog4netGlobal()))
            {
                var data = (from fcsm in XPOFunction.XPQueryCatched<FIDEL_ClienteSesionMaquina>(uow, x => (extract.rowVersion == null || x.RowVersion.Compare(extract.rowVersion) > 0) && x.PartidasJugadas > 0 && typeState.Contains(x.Estado.CodEstado)
                            && (extract.startDate == null || x.FechaFacturacion >= extract.startDate) && (extract.endDate == null || x.FechaFacturacion <= extract.endDate), Logger.Logger.GetLog4netGlobal())
                            join fcd in XPOFunction.XPQueryCatched<FIDEL_CLIENTE_Datos>(uow, Logger.Logger.GetLog4netGlobal()) on fcsm.IDCliente.IDCliente equals fcd.IDCliente
                            join fcs in XPOFunction.XPQueryCatched<FIDEL_CLIENTE_Saldo>(uow, Logger.Logger.GetLog4netGlobal()) on fcd.IDCliente equals fcs.IDCliente
                            join fcdet in XPOFunction.XPQueryCatched<FIDEL_CLIENTE_Detalles>(uow, Logger.Logger.GetLog4netGlobal()) on fcd.IDCliente equals fcdet.IDCliente
                            join anw in XPOFunction.XPQueryCatched<ACCOUNTING_NetWin>(uow, Logger.Logger.GetLog4netGlobal()) on new { fcsm.IDMaquina.IDMaquina, fcsm.FechaFacturacion } equals new { anw.CompoundKey1.IDMaquina.IDMaquina, anw.CompoundKey1.FechaFacturacion }
                            orderby fcsm.NroSesion
                            select new ExtractedCategoryModel
                            {
                                machineId = fcsm.IDMaquina.IDMaquina,
                                idClient = fcs.IDCliente,
                                client = string.Format("{0} {1} {2}", fcd.ApellidoPaterno, fcd.ApellidoMaterno, fcd.Nombre),
                                gender = fcd.Sexo.IDSexo,
                                age = DateFunction.GetAge((DateTime)fcd.FechaNacimiento),
                                birthDate = (DateTime)fcd.FechaNacimiento,
                                email = fcd.Email,
                                observations = fcdet.Observaciones,
                                cellPhone = fcd.TelefonoCelular,
                                billingDate = (DateTime)fcd.FechaHoraAlta,
                                businessLine = businessLine,
                                accountingDate = fcsm.FechaFacturacion,
                                typeClient = fcd.CodTipoCliente.Descripcion,
                                clientFidelPoints = fcs.SaldoFidel,
                                totalBet = fcsm.Apostado,
                                totalWin = fcsm.Ganado,
                                totalJackpot = fcsm.Jackpots,
                                totalNetWin = fcsm.Apostado - (fcsm.Ganado + fcsm.Jackpots),
                                hold = 0,
                                gamesPlayed = fcsm.PartidasJugadas,
                                gamesWon = fcsm.PartidasGanadas,
                                gamesLost = fcsm.PartidasPerdidas,
                                totalPoints = fcsm.PuntosFidelSumados,
                                pointsRedeemed = 0,
                                totalTimeSesion = System.Data.Linq.SqlClient.SqlMethods.DateDiffMinute(fcsm.FechaHoraInicio, fcsm.FechaHoraFin.Value),
                                rowVersion = fcsm.RowVersion,
                                nroSesion = fcsm.NroSesion,
                                rowVersionLong = LongFunction.GetRowversion(fcsm.RowVersion),
                                paybackPercentage = anw.PaybackPercentage,
                                typeState = anw.CompoundKey1.Estado.CodTipoEstado
                            }).Skip(((int)extract.pageNumber - 1) * (int)result.pageSize).Take((int)result.pageSize).ToList();
                var billedRecords = data.Where(x => x.typeState == 'F').ToList();
                var filteredRecords = data.Where(x => !billedRecords.Any(y => x.machineId == y.machineId && x.accountingDate == y.accountingDate && x.typeState != 'F')).ToList();
                return filteredRecords;
            }
        }

        /// <summary>
        /// Method to obtain generic list
        /// </summary>
        /// <param name="result">Get data</param>
        /// <returns>Generic list machine sessions.</returns>
        private List<ExtractedCategoryModel> CreateListData(List<ExtractedCategoryModel> result)
        {
            var list = result.GroupBy(x => new
            {
                x.machineId,
                x.idClient,
                x.accountingDate
            }).Select(item => new ExtractedCategoryModel
            {
                machineId = item.Key.machineId,
                idClient = item.Key.idClient,
                client = item.FirstOrDefault().client,
                gender = item.FirstOrDefault().gender,
                age = item.FirstOrDefault().age,
                birthDate = item.FirstOrDefault().birthDate,
                email = item.FirstOrDefault().email,
                observations = item.FirstOrDefault().observations,
                cellPhone = item.FirstOrDefault().cellPhone,
                billingDate = item.FirstOrDefault().billingDate,
                businessLine = item.FirstOrDefault().businessLine,
                accountingDate = item.FirstOrDefault().accountingDate,
                typeClient = item.FirstOrDefault().typeClient,
                clientFidelPoints = item.Sum(x => x.clientFidelPoints),
                totalBet = item.Sum(x => x.totalBet),
                totalWin = item.Sum(x => x.totalWin),
                totalJackpot = item.Sum(x => x.totalJackpot),
                totalNetWin = item.Sum(x => x.totalBet) - (item.Sum(x => x.totalWin) + item.Sum(x => x.totalJackpot)),
                hold = DecimalFunction.CalculateHold(item.Sum(x => (decimal)x.totalBet), item.Sum(x => (decimal)x.totalWin), item.Sum(x => (decimal)x.totalJackpot)),
                gamesPlayed = item.Sum(x => x.gamesPlayed),
                gamesWon = item.Sum(x => x.gamesWon),
                gamesLost = item.Sum(x => x.gamesLost),
                totalPoints = item.Sum(x => x.totalPoints),
                pointsRedeemed = 0,
                totalTimeSesion = item.Sum(x => x.totalTimeSesion),
                rowVersion = item.FirstOrDefault().rowVersion,
                rowVersionLong = item.FirstOrDefault().rowVersionLong
            }).ToList();
            return list;
        }

        /// <summary>
        /// Method to obtain ExtractedCategoryModel.
        /// </summary>
        /// <param name="list">Generic list</param>
        /// <returns>Extracted category model</returns>
        private List<ExtractedCategoryModel> GetExtactedCategoryModel(List<ExtractedCategoryModel> list)
        {
            List<ExtractedCategoryModel> listClientsCategoryReturn = new List<ExtractedCategoryModel>();
            listClientsCategoryReturn = list.GroupBy(x => new
            {
                idClient = x.idClient,
                accountingDate = x.accountingDate
            }).Select(item => new ExtractedCategoryModel
            {
                idClient = item.Key.idClient,
                client = item.FirstOrDefault().client,
                gender = item.FirstOrDefault().gender,
                age = item.FirstOrDefault().age,
                birthDate = item.FirstOrDefault().birthDate,
                email = item.FirstOrDefault().email,
                observations = item.FirstOrDefault().email,
                cellPhone = item.FirstOrDefault().cellPhone,
                billingDate = item.FirstOrDefault().billingDate,
                businessLine = item.FirstOrDefault().businessLine,
                accountingDate = item.FirstOrDefault().accountingDate,
                typeClient = item.FirstOrDefault().typeClient,
                clientFidelPoints = item.Sum(x => x.clientFidelPoints),
                totalBet = item.Sum(x => x.totalBet),
                totalWin = item.Sum(x => x.totalWin),
                totalJackpot = item.Sum(x => x.totalJackpot),
                totalNetWin = item.Sum(x => x.totalNetWin),
                hold = item.Sum(x => x.hold),
                gamesPlayed = item.Sum(x => x.gamesPlayed),
                gamesWon = item.Sum(x => x.gamesWon),
                gamesLost = item.Sum(x => x.gamesLost),
                totalPoints = item.Sum(x => x.totalPoints),
                pointsRedeemed = 0,
                totalTimeSesion = item.Sum(x => x.totalTimeSesion),
                rowVersion = item.FirstOrDefault().rowVersion,
                rowVersionLong = item.FirstOrDefault().rowVersionLong
            }).ToList();
            return listClientsCategoryReturn;
        }
    }
}