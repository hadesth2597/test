using log4net;
using PLAYTRAK.LocalDBModel.Models.Loyalty;
using PLAYTRAK.LocalDBModel.XPOModel.PLAYTRAK.LocalDBModel;
using PLAYTRAK.ReporteFidelizacion;
using PLAYTRAK.ReportesFidelizacion.Resources;
using PLAYTRAK.XPOHelper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace PLAYTRAK.ReportesFidelizacion.Models
{
    /// <summary>
    /// Returns details of loyanty customer session
    /// Author: Enrique Espeleta Vega
    /// Date: 2019-07-12
    /// </summary>
    public class LoyaltySessionClientDetailModel
    {
        /// <summary>
        /// Method designed to return details of customer loyalty sessions for both business lines (Machines/Tables).
        /// </summary>
        /// <param name="startDate">Start date for the loyalty client details search.</param>
        /// <param name="endDate">End date for the loyalty client details search.</param>
        /// <param name="idClient">Optional client identifier.</param>
        /// <param name="slotMachineId">Optional slot machine identifier.</param>
        /// <param name="gameTableId">Optional game table identifier.</param>
        /// <returns>A response containing loyalty client details.</returns>
        public Response GetLoyaltyClientDetails(DateTime startDate, DateTime endDate, long? idClient, short? slotMachineId, short? gameTableId)
        {
            string MethodName = MethodBase.GetCurrentMethod().Name;
            string message = String.Format(PlayTrak.StartExecuteMethod, MethodName);
            Logger.Logger.GetLog4netGlobal().Info(message);
            Response resp = new Response() { TypeOfResponse = TypeOfResponse.Ok };
            try
            {
                if (startDate <= DateTime.MinValue || endDate <= DateTime.MinValue)
                {
                    resp.TypeOfResponse = TypeOfResponse.Error;
                    resp.Message = ErrorMessages.MinimumRequired;
                    return resp;
                }
                else
                {
                    using (var uow = XPOFunction.GetNewUnitOfWork(Logger.Logger.GetLog4netGlobal()))
                    {
                        List<LoyaltyCustomerSessions> slotMachine = new List<LoyaltyCustomerSessions>();
                        List<LoyaltyCustomerSessions> tableGame = new List<LoyaltyCustomerSessions>();
                        if ((slotMachineId != null && gameTableId == null) || (slotMachineId == null && gameTableId == null) || (slotMachineId != null && gameTableId != null))
                        {
                            slotMachine = SlotMachineSessions(idClient, slotMachineId, gameTableId, startDate, endDate);
                        }
                        if ((gameTableId != null && slotMachineId == null) || (slotMachineId == null && gameTableId == null) || (slotMachineId != null && gameTableId != null))
                        {
                            tableGame = TableGameSessions(idClient, slotMachineId, gameTableId, startDate, endDate);
                        }
                        IEnumerable<LoyaltyCustomerSessions> result = slotMachine.Union(tableGame);
                        resp.Data = result.ToList();
                        resp.TypeOfResponse = TypeOfResponse.Ok;
                    }
                }
            }
            catch (Exception ex)
            {
                resp.TypeOfResponse = TypeOfResponse.Error;
                resp.Message = ErrorMessages.MinimumRequired;
                message = string.Format(PlayTrak.ErrorExecuteMethod, MethodName);
                Logger.Logger.GetLog4netGlobal().Error(message, ex);
            }
            return resp;
        }

        /// <summary>
        /// Valid states types
        /// </summary>
        List<char> states = new List<char>() { 'F', 'C', 'D', 'P', 'U' };

        /// <summary>
        /// Get Slot Machine session
        /// </summary>
        /// <param name="idClient">Customer Id</param>
        /// <param name="slotMachineId">Slot Machine Id</param>
        /// <param name="gameTableId">Terminal Table Id</param>
        /// <param name="startDate">Initial date</param>
        /// <param name="endDate">End Date</param>
        /// <returns></returns>
        private List<LoyaltyCustomerSessions> SlotMachineSessions(long? idClient, short? slotMachineId, short? gameTableId, DateTime startDate, DateTime endDate)
        {
            List<LoyaltyCustomerSessions> SlotMachine = new List<LoyaltyCustomerSessions>();
            if (idClient == null && gameTableId == null && slotMachineId == null)
            {
                return SlotMachine;
            }
            using (var uow = XPOFunction.GetNewUnitOfWork(Logger.Logger.GetLog4netGlobal()))
            {
                var businessLine = XPOFunction.XPQueryCatched<CASHLESS_TipoClienteSaldo>(uow, a => a.CodTipoSaldo == 'S', Logger.Logger.GetLog4netGlobal()).FirstOrDefault().Descripcion;
                SlotMachine = (from fcsm in XPOFunction.XPQueryCatched<FIDEL_ClienteSesionMaquina>(uow, a => a.FechaFacturacion >= startDate && a.FechaFacturacion <= endDate &&
                                                                                                        (idClient == null || a.IDCliente.IDCliente == idClient) &&
                                                                                                        (slotMachineId == null || a.IDMaquina.IDMaquina == slotMachineId) &&
                                                                                                        states.Contains(a.Estado.CodEstado)
                                                                                                      , Logger.Logger.GetLog4netGlobal())
                               join fcd in XPOFunction.XPQueryCatched<FIDEL_CLIENTE_Datos>(uow, Logger.Logger.GetLog4netGlobal())
                                   on new { fcsm.IDCliente.IDCliente } equals new { fcd.IDCliente }
                               join ff in XPOFunction.XPQueryCatched<FIDEL_Formula>(uow, Logger.Logger.GetLog4netGlobal())
                                   on new { fcsm.IDFormulaAplicada.IDFormula } equals new { ff.IDFormula }
                               join fctc in XPOFunction.XPQueryCatched<FIDEL_CLIENTE_TipoCliente>(uow, Logger.Logger.GetLog4netGlobal())
                                   on new { fcd.CodTipoCliente.CodTipoCliente } equals new { fctc.CodTipoCliente }
                               orderby fcsm.NroSesion descending
                               select new LoyaltyCustomerSessions
                               {
                                   SessionNumber = fcsm.NroSesion,
                                   CustomerId = fcsm.IDCliente.IDCliente,
                                   CardId = fcsm.IDTarjeta.IDTarjeta,
                                   SlotMachineId = fcsm.IDMaquina.IDMaquina,
                                   BillingDate = fcsm.FechaFacturacion,
                                   InitialDateTime = fcsm.FechaHoraInicio,
                                   EndDateTime = (fcsm.FechaHoraFin.HasValue ? fcsm.FechaHoraFin.Value : fcsm.FechaHoraFin),
                                   State = fcsm.Estado.Descripcion,
                                   Bet = fcsm.Apostado,
                                   Won = fcsm.Ganado,
                                   Jackpots = fcsm.Jackpots,
                                   GamesPlayed = fcsm.PartidasJugadas,
                                   Bills = fcsm.BilletesInsertados,
                                   WonGames = fcsm.PartidasGanadas,
                                   LostGames = fcsm.PartidasPerdidas,
                                   PromRestrictedPlayed = fcsm.PromRestrictedJugados,
                                   PromNonRestrictedPlayed = fcsm.PromNonRestrictedJugados,
                                   FormulaAppliedId = fcsm.IDFormulaAplicada.IDFormula,
                                   LoyaltyPointsTotal = fcsm.PuntosFidelSumados,
                                   GameTime = System.Data.Linq.SqlClient.SqlMethods.DateDiffMinute(fcsm.FechaHoraInicio, fcsm.FechaHoraFin.Value),
                                   CustomerType = fcd.CodTipoCliente.Descripcion,
                                   CustomerCompleteName = (fcd.ApellidoPaterno ?? "") + " " + (fcd.ApellidoMaterno ?? "") + " " + (fcd.Nombre ?? ""),
                                   Name = (fcd.Nombre ?? ""),
                                   FathersName = (fcd.ApellidoPaterno ?? ""),
                                   MothersName = (fcd.ApellidoMaterno ?? ""),
                                   FormulaAppliedName = ff.Descripcion,
                                   CustomerNameColor = fctc.ColorIdentificador,
                                   BusinessLine = businessLine
                               }).ToList();
            }
            return SlotMachine;
        }

        /// <summary>
        /// Get table game sessions
        /// </summary>
        /// <param name="idClient">Customer Identifcation</param>
        /// <param name="slotMachineId">Slot Machine Id</param>
        /// <param name="gameTableId">Game Table Identification</param>
        /// <param name="startDate">Initial Date</param>
        /// <param name="endDate">End Date</param>
        /// <returns></returns>
        private List<LoyaltyCustomerSessions> TableGameSessions(long? idClient, short? slotMachineId, short? gameTableId, DateTime startDate, DateTime endDate)
        {
            List<LoyaltyCustomerSessions> TableGame = new List<LoyaltyCustomerSessions>();
            if (idClient == null && gameTableId == null && slotMachineId == null)
            {
                return TableGame;
            }
            using (var uow = XPOFunction.GetNewUnitOfWork(Logger.Logger.GetLog4netGlobal()))
            {
                var businessLine = XPOFunction.XPQueryCatched<CASHLESS_TipoClienteSaldo>(uow, a => a.CodTipoSaldo == 'M', Logger.Logger.GetLog4netGlobal()).FirstOrDefault().Descripcion;
                TableGame = (from fcsm in XPOFunction.XPQueryCatched<FIDEL_ClienteSesionMesa>(uow, a => a.FechaFacturacion >= startDate && a.FechaFacturacion <= endDate &&
                                                                                                   (idClient == null || a.IDCliente.IDCliente == idClient) &&
                                                                                                   (gameTableId == null || a.IDMesa.IDMesa == gameTableId) &&
                                                                                                   states.Contains(a.Estado.CodEstado)
                                                                                                 , Logger.Logger.GetLog4netGlobal())
                             join fcd in XPOFunction.XPQueryCatched<FIDEL_CLIENTE_Datos>(uow, Logger.Logger.GetLog4netGlobal())
                             on new { fcsm.IDCliente.IDCliente } equals new { fcd.IDCliente }
                             join ff in XPOFunction.XPQueryCatched<FIDEL_Formula>(uow, Logger.Logger.GetLog4netGlobal())
                             on new { fcsm.IDFormulaAplicada.IDFormula } equals new { ff.IDFormula }
                             join fctc in XPOFunction.XPQueryCatched<FIDEL_CLIENTE_TipoCliente>(uow, Logger.Logger.GetLog4netGlobal())
                             on new { fcd.CodTipoCliente.CodTipoCliente } equals new { fctc.CodTipoCliente }
                             orderby fcsm.NroSesion descending
                             select new LoyaltyCustomerSessions
                             {
                                 SessionNumber = fcsm.NroSesion,
                                 CustomerId = fcsm.IDCliente.IDCliente,
                                 CardId = fcsm.IDTarjeta.IDTarjeta,
                                 SlotMachineId = fcsm.IDMesa.IDMesa,
                                 TableSeatPosition = (int)fcsm.Posicion,
                                 BillingDate = fcsm.FechaFacturacion,
                                 InitialDateTime = fcsm.FechaHoraInicio,
                                 EndDateTime = (fcsm.FechaHoraFin.HasValue ? fcsm.FechaHoraFin.Value : fcsm.FechaHoraFin),
                                 State = fcsm.Estado.Descripcion,
                                 Bet = fcsm.Apostado,
                                 BetAverage = fcsm.PromApuesta,
                                 Won = fcsm.Ganado,
                                 Jackpots = fcsm.Jackpots,
                                 GamesPlayed = fcsm.PartidasJugadas,
                                 EstimateGamesPlayed = fcsm.PartidasJugadasEstimado,
                                 DropIn = fcsm.DropIn,
                                 FormulaAppliedId = fcsm.IDFormulaAplicada.IDFormula,
                                 LoyaltyPointsTotal = fcsm.PuntosFidelSumados,
                                 GameTime = System.Data.Linq.SqlClient.SqlMethods.DateDiffMinute(fcsm.FechaHoraInicio, fcsm.FechaHoraFin.Value),
                                 CustomerType = fcd.CodTipoCliente.Descripcion,
                                 CustomerCompleteName = (fcd.ApellidoPaterno ?? "") + " " + (fcd.ApellidoMaterno ?? "") + " " + (fcd.Nombre ?? ""),
                                 Name = (fcd.Nombre ?? ""),
                                 FathersName = (fcd.ApellidoPaterno ?? ""),
                                 MothersName = (fcd.ApellidoMaterno ?? ""),
                                 FormulaAppliedName = ff.Descripcion,
                                 CustomerNameColor = fctc.ColorIdentificador,
                                 BusinessLine = businessLine,
                                 Username = fcsm.Username,
                                 TableTerminal = fcsm.IDMesa.Descripcion
                             }).ToList();
            }
            return TableGame;
        }
    }
}