from zeep import Client
from zeep.transports import Transport
from requests import Session
from lxml import etree
from decimal import Decimal
import logging
from modelos.data_access import find_pettycash, receivable_consultation
from dominus_sales_api.config import wsdl_rec
wsdl = wsdl_rec
session = Session()
session.verify = False  # Cambiar a True si tienes un certificado SSL válido
transport = Transport(session=session)
client = Client(wsdl=wsdl, transport=transport)

# Configuración del logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("app.log"),
                        logging.StreamHandler()
                    ])

logger = logging.getLogger(__name__)

def prepare_lTSTsRdtca_data():
    try:
        lTSTsRdtca = {
            'TOTsRdtca': [
                {
                    'Imp_codi': 1001,
                    'Dst_codi': 0,
                    'Rdt_imds': 'I',
                    'Arb_csuc': '10810001',
                    'Rdt_valo': 0  # 100.0
                }
            ]
        }
        logger.info("Datos de lTSTsRdtca preparados exitosamente.")
        return lTSTsRdtca
    except Exception as e:
        logger.error(f"Error preparando lTSTsRdtca: {e}", exc_info=True)

def prepare_lTOTsDreca_data(Ctaxcobrar):
    try:
        lTOTsDreca = {
            'TOTsDreca': [
                {
                    'cxc_nume': Ctaxcobrar[0]['cxc_nume'],
                    'cxc_nech': Ctaxcobrar[0]['cxc_nech'],
                    'arb_csuc': '443',
                    'top_ccxc': 3410,
                    'cxc_cont': Ctaxcobrar[0]['cxc_cont'],
                    'cxc_cref': Ctaxcobrar[0]['cxc_cref'],
                    'rts_valo': Ctaxcobrar[0]['cxc_tota'],
                    # 'lTSTsRdtca': prepare_lTSTsRdtca_data()  # Incluye los datos de lTSTsRdtca ( validar por conytabilidad )
                }
            ]
        }
        logger.info("Datos de lTOTsDreca preparados exitosamente.")
        return lTOTsDreca
    except Exception as e:
        logger.error(f"Error preparando lTOTsDreca: {e}", exc_info=True)

def prepare_lTOTsDfopa_data(Ctaxcobrar):
    try:
        lTOTsDfopa = {
            'TOTsDfopa': [
                {
                    'fop_codi': 101,
                    'dfo_valo': Ctaxcobrar[0]['cxc_tota'],
                    'ban_codi': 1,
                    'tac_codi': 1,
                    'dfo_fech': Ctaxcobrar[0]['cxc_nech'],
                    'dfo_cedu': 123456789,
                    'dfo_nomg': 'N.A',
                    'dfo_vcom': 0.0,
                    'dfo_viva': 0.0
                }
            ]
        }
        logger.info("Datos de lTOTsDfopa preparados exitosamente.")
        return lTOTsDfopa
    except Exception as e:
        logger.error(f"Error preparando lTOTsDfopa: {e}", exc_info=True)
        
        
        

def prepare_ts_recaj_data(vDetalle, fecha_recaudo, cfl_codi,fac_cref):
    try:
        # Consulta la cuenta por cobrar
        Ctaxcobrar = receivable_consultation(vDetalle[0]['id_proyecto'], fac_cref)
        
        # Si la cuenta por cobrar no está vacía, prepara los datos
        if Ctaxcobrar:
            toTsRecaj = {
                'emp_codi': vDetalle[0]['id_proyecto'],
                'top_codi': 1040,
                'mte_nume': 0,
                'mte_fech': fecha_recaudo.strftime('%Y%m%d'),  # Formato de fecha: YYYYMMDD
                'arb_csuc': vDetalle[0]['sucursal'],
                'mte_desc': Ctaxcobrar[0]['cxc_desc'],
                'ter_coda': vDetalle[0]['cli_coda'],
                'cfl_codi': cfl_codi,
                'mon_codi': 1,
                'mte_feta': fecha_recaudo.strftime('%Y%m%d'),
                'caj_codi': find_pettycash(vDetalle[0]['id_proyecto'], vDetalle[0]['operacion']),
                'ven_codi': 0,
                'lTOTsDreca': prepare_lTOTsDreca_data(Ctaxcobrar),
                'lTOTsDfopa': prepare_lTOTsDfopa_data(Ctaxcobrar),
            }
            logger.info("Datos de ts_recaj preparados exitosamente.")
            return toTsRecaj
        
        # Si no se encontró cuenta por cobrar, registra un mensaje informativo
        else:
            logger.info(f"No existe cuenta por cobrar para la factura {fac_cref} del proyecto {vDetalle[0]['id_proyecto']}.")
            return None  # Puedes ajustar esto si quieres que devuelva algo más en este caso.

    except Exception as e:
        # Registra el error con toda la información de la excepción
        logger.error(f"Error preparando ts_recaj_data para la factura {fac_cref}: {e}", exc_info=True)
        raise  # Vuelve a lanzar la excepción si quieres que se gestione en otro nivel


def payment_collection(toTsRecaj_data):
    try:
        # Llamada al servicio
        response = client.service.InsertarTsRecaj(toTsRecaj_data)
        
        # Verifica si 'Mte_Cont' es None y registra un error si es el caso
        if response['Mte_Cont'] is None:
            error_message = f"Error en la inserción de ts_recaj: {response['Txterror']}"
            logger.error(error_message)
            raise ValueError(f"Error de servicio: {error_message}")
        
        # Si todo está bien, registra el éxito
        return response

    except Exception as e:
        # Captura cualquier otro error y lo registra
        logger.error(f"Error al insertar ts_recaj: {e}", exc_info=True)
        raise
    
        
        
        

if __name__ == "__main__":
    # Preparar los datos del recibo de caja
    toTsRecaj_data = prepare_ts_recaj_data()
    
    # Llamar al servicio web con los datos preparados
    response = client.service.InsertarTsRecaj(toTsRecaj_data)
    
    # Registrar el resultado de la llamada al servicio web
    logger.info(f"Response from InsertarTsRecaj: {response}")


      
