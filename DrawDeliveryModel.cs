using DevExpress.Xpo;
using log4net;
using PLAYTRAK.LocalDBModel.Models;
using PLAYTRAK.LocalDBModel.XPOModel.PLAYTRAK.LocalDBModel;
using PLAYTRAK.ReportesFidelizacion.Models;
using PLAYTRAK.ReportesFidelizacion.Resources;
using PLAYTRAK.XPOHelper;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace PLAYTRAK.ReportesFidelizacion.Models
{
    public class DrawDeliveryModel
    {
        /// <summary>
        /// Id draw
        /// </summary>
        public int IdDraw { get; set; }
        /// <summary>
        /// Id client
        /// </summary>
        public int IdClient { get; set; }
        /// <summary>
        /// Name of draw
        /// </summary>
        public string DrawName { get; set; }
        /// <summary>
        /// Type of draw
        /// </summary>
        public string TypeDraw { get; set; }
        /// <summary>
        /// Name of client
        /// </summary>
        public string Client { get; set; }
        /// <summary>
        /// TicketNumber
        /// </summary>
        public int TicketNumber { get; set; }
        /// <summary>
        /// Date of purchase
        /// </summary>
        public DateTime DatePurchase { get; set; }
        /// <summary>
        /// Category of client
        /// </summary>
        public String CategoryClient { get; set; }
        /// <summary>
        /// Voucher id
        /// </summary>
        public int VoucherID { get; set; }
        /// <summary>
        /// Start date
        /// </summary>
        public DateTime? StartDate { get; set; }
        /// <summary>
        /// End date
        /// </summary>
        public DateTime? EndDate { get; set; }
        /// <summary>
        /// Active estatus
        /// </summary>
        public String Active { get; set; }
        /// <summary>
        /// Draw operation Date and time (in UTC)
        /// </summary>
        public DateTime OperationDateTime { get; set; }
        /// <summary>
        /// Amount of points
        /// </summary>
        public decimal? OperationPointsAmount { get; set; }

        /// <summary>
        /// Method for retrieving information from the lottery report, using the provided dates.
        /// </summary>
        /// <param name="idDraw">Optional draw identifier.</param>
        /// <param name="idClient">Optional client identifier.</param>
        /// <param name="idTicket">Optional ticket identifier.</param>
        /// <param name="startDate">Optional start date for the draw delivery report.</param>
        /// <param name="endDate">Optional end date for the draw delivery report.</param>
        /// <returns>A response containing the draw delivery report.</returns>
        public Response GetDrawDeliveryReport(int? idDraw, int? idClient, int? idTicket, DateTime? startDate, DateTime? endDate)
        {
            string MethodName = MethodBase.GetCurrentMethod().Name;
            string message = String.Format(PlayTrak.StartExecuteMethod, MethodName);
            Logger.Logger.GetLog4netGlobal().Info(message);
            Response response = new Response() { TypeOfResponse = TypeOfResponse.Ok };
            List<DrawDeliveryModel> listDraws = null;
            try
            {
                if (startDate == null || endDate == null)
                {
                    response.TypeOfResponse = TypeOfResponse.Other;
                    response.Message = ErrorMessages.DateRequired;
                    return response;
                }

                if (startDate > endDate)
                {
                    response.TypeOfResponse = TypeOfResponse.Other;
                    response.Message = ErrorMessages.DateIsGreaterThan;
                    return response;
                }

                using (UnitOfWork uow = XPOFunction.GetNewUnitOfWork(Logger.Logger.GetLog4netGlobal()))
                {

                    listDraws = (from fcsm in XPOFunction.XPQueryCatched<FIDEL_OperationDrawTicket>(uow, x => x.OperationNumber.FechaFacturacion >= startDate && x.OperationNumber.FechaFacturacion <= endDate &&
                                           (idDraw == null || x.CompoundKey1.DrawID.DrawID == idDraw) &&
                                           (idClient == null || x.OperationNumber.IDCliente.IDCliente == idClient) &&
                                           (idTicket == null || x.CompoundKey1.TicketNumber == idTicket), Logger.Logger.GetLog4netGlobal())
                                 join fcd in XPOFunction.XPQueryCatched<FIDEL_CLIENTE_Datos>(uow, Logger.Logger.GetLog4netGlobal())
                                     on new { fcsm.OperationNumber.IDCliente.IDCliente } equals new { fcd.IDCliente }
                                 join ff in XPOFunction.XPQueryCatched<FIDEL_OperationVoucher>(uow, Logger.Logger.GetLog4netGlobal())
                                     on new { fcsm.OperationNumber.NroOperacion } equals new { ff.OperationNumber.NroOperacion }
                                 where ff.VoucherID != 0
                                 select new DrawDeliveryModel
                                 {
                                     IdDraw = fcsm.CompoundKey1.DrawID.DrawID,
                                     DrawName = fcsm.CompoundKey1.DrawID.ShortName,
                                     TypeDraw = fcsm.CompoundKey1.DrawID.TypeDrawCode.ShortName,
                                     Client = string.Format("{0} {1} {2}", fcd.ApellidoPaterno, fcd.ApellidoMaterno, fcd.Nombre),
                                     TicketNumber = fcsm.CompoundKey1.TicketNumber,
                                     DatePurchase = fcsm.OperationNumber.FechaFacturacion,
                                     CategoryClient = fcsm.OperationNumber.IDCliente.CodTipoCliente.Descripcion,
                                     VoucherID = ff.VoucherID,
                                     StartDate = fcsm.CompoundKey1.DrawID.DateTimeStart,
                                     EndDate = fcsm.CompoundKey1.DrawID.DateTimeEnd,
                                     Active = fcsm.CompoundKey1.DrawID.Active ? "Activo" : "Inactivo",
                                     IdClient = fcsm.OperationNumber.IDCliente.IDCliente,
                                     OperationDateTime = fcsm.OperationNumber.FechaHora,
                                     OperationPointsAmount = fcsm.OperationNumber.ImportePuntos,
                                 }).ToList();
                    response.Data = listDraws;
                }
            }
            catch (Exception ex)
            {
                response.TypeOfResponse = TypeOfResponse.Error;
                response.Message = ex.Message;
                message = string.Format(PlayTrak.ErrorExecuteMethod, MethodName);
                Logger.Logger.GetLog4netGlobal().Error(message, ex);
            }
            return response;
        }

        /// <summary>
        /// Method to retrieve pertinent information about draws, including metrics such as the count of active draws, the total number of draws, etc.
        /// </summary>
        /// <returns>A response containing card information obtained.</returns>
        public Response GetCard()
        {
            string MethodName = MethodBase.GetCurrentMethod().Name;
            string message = String.Format(PlayTrak.StartExecuteMethod, MethodName);
            Logger.Logger.GetLog4netGlobal().Info(message);
            Response response = new Response() { TypeOfResponse = TypeOfResponse.Ok };
            try
            {
                DateTime fechaFacturacion = DefaultModel.ObtenerFechaFacturacion();
                GetCardModel getCardModel = new GetCardModel();
                using (UnitOfWork uow = XPOFunction.GetNewUnitOfWork(Logger.Logger.GetLog4netGlobal()))
                {
                    var time = XPOFunction.ExecServerUtcTime(uow);
                    var zone = LocalDBModel.Functions.TimeZone.GetTimeZoneDB(uow);
                    var IdDrawsActive = XPOFunction.XPQueryCatched<FIDEL_Draw>(uow, x => x.Active && x.DateTimeStart.Value <= time && x.DateTimeEnd.Value >= time, Logger.Logger.GetLog4netGlobal()).Select(x => x.DrawID).ToList();
                    var IdDrawsTodayList = XPOFunction.XPQueryCatched<FIDEL_Draw>(uow, x => x.DateTimeEnd.Value > time.Date.AddDays(-2) && x.DateTimeEnd.Value < time.Date.AddDays(2) && x.Active, Logger.Logger.GetLog4netGlobal()).ToList();
                    foreach (var item in IdDrawsTodayList)
                    {
                        item.DateTimeEnd = LocalDBModel.Functions.TimeZone.GetLocalFromUTC(item.DateTimeEnd.Value, zone);
                    }
                    var IdDrawsToday = IdDrawsTodayList.Where(x => x.DateTimeEnd.Value.Date == fechaFacturacion.Date).Select(x => x.DrawID).ToList();
                    getCardModel.TotalActive = IdDrawsActive.Count();
                    getCardModel.TotalToday = IdDrawsToday.Count();
                    getCardModel.IdDrawsActive = IdDrawsActive;
                    getCardModel.IdDrawsToday = IdDrawsToday;
                    response.Data = getCardModel;
                    response.Message = "OK";
                }
            }
            catch (Exception ex)
            {
                response.TypeOfResponse = TypeOfResponse.Error;
                response.Message = ex.Message;
                message = string.Format(PlayTrak.ErrorExecuteMethod, MethodName);
                Logger.Logger.GetLog4netGlobal().Error(message, ex);
            }
            return response;
        }
    }

    public class GetCardModel
    {
        /// <summary>
        /// Total draws active
        /// </summary>
        public int TotalActive { get; set; }
        /// <summary>
        /// List draws id active
        /// </summary>
        public List<short> IdDrawsActive { get; set; }
        /// <summary>
        /// Total draws today
        /// </summary>
        public int TotalToday { get; set; }
        /// <summary>
        /// List draws id today
        /// </summary>
        public List<short> IdDrawsToday { get; set; }
    }
}
