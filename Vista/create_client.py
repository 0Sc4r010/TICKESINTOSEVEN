from logging.handlers import TimedRotatingFileHandler
from zeep import Client
from zeep.transports import Transport
from requests import Session
import re
from Vista.business_rationale import extraer_fac_cont
from modelos.data_access import view_invoice_customer_data
from dominus_sales_api.config import wsdl_ews
import logging

wsdl =  wsdl_ews
session = Session()
session.verify = False  # Cambiar a True si tienes un certificado SSL válido
transport = Transport(session=session)
client = Client(wsdl=wsdl, transport=transport)


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        TimedRotatingFileHandler("app.log", when="midnight", interval=1, backupCount=7),
                        logging.StreamHandler()
                    ])

logger = logging.getLogger(__name__)


def identificar_documento(numero):
    try:
        numero = int(numero)  # Asegurar que sea un número entero

        # Definir el rango válido de NITs
        rango_min_nit = 800000000
        rango_max_nit = 999999999

        # Validar si está en el rango de NITs
        if rango_min_nit <= numero <= rango_max_nit:
            return 31  # Código para NITs
        else:
            return 13  # Código para cédulas

    except ValueError:
        raise ValueError(f"El valor proporcionado no es un número válido: {numero}")


def calcular_digito_verificacion(nit):
    # Pesos definidos por la DIAN
    pesos = [3, 7, 13, 17, 19, 23, 29, 37, 41, 43, 47, 53, 59, 67, 71]
    
    # Convertir el NIT a una cadena para iterar sobre los dígitos
    nit = str(nit)
    
    # Validar que el NIT sea numérico
    if not nit.isdigit():
        raise ValueError("El NIT debe ser un número válido.")
    
    # Calcular la suma ponderada de los dígitos del NIT
    suma = sum(int(digito) * peso for digito, peso in zip(nit[::-1], pesos))
    
    # Calcular el residuo módulo 11
    residuo = suma % 11
    
    # Determinar el DV
    if residuo in (0, 1):
        return residuo
    return 11 - residuo



def procesar_customer_data(cursor):
    results = []  # Lista para almacenar todas las respuestas de los clientes procesados
    
    for row in cursor:
        try:
            # Llamada al servicio web SyncFaClien con los datos del cliente
            customer_response = client.service.SyncFaClien(
                emp_codi=row['id_proyecto'],
                tip_codi=identificar_documento(row['cli_coda']),
                ter_coda=row['cli_coda'],
                cli_dive= calcular_digito_verificacion(row['cli_coda']) if identificar_documento(row['cli_coda']) == 31 else 0,
                cli_nomb=row['cli_name'],
                cli_apel=row['cli_lasn'],
                cli_noco=row['cli_noco'],
                mod_codi=row['mod_codi'],
                pai_codi=row['pai_codi'],
                dep_codi=row['dep_codi'],
                mun_codi=row['mun_codi'],
                cli_coda=row['cli_coda'],
                tcl_codi=row['tcl_codi'],
                cal_codi=0,
                coc_codi=row['coc_codi'],
                cim_codi=row['cim_codi'],
                arb_csuc=row['sucursal'],
                dcl_dire=row['cli_addr'],
                dcl_ntel=row['cli_phon'],
                dcl_mail=row['eml_clte'],
                dcl_nfax=row['cli_phon'],
                arb_clte=1,
                cli_inna='S',
                ven_codi=row['ven_codi'],
                arb_ccec=row['ctro_Costo'],
                lis_codi=0,
                dcl_obse=row['des_fact'],
                cli_fecm=0,
                cli_feca=0
            )
            
            # Procesar la respuesta del servicio
            if extraer_fac_cont('RETORNO', customer_response) != 0:
                logger.info(f"Error procesando cliente {row['cli_coda']}: {customer_response}", exc_info=True)
                    
            # Agregar la respuesta al listado de resultados
            results.append(customer_response)
            
        except Exception as e:
            # Manejo de errores para capturar excepciones en la llamada al servicio web
            logger.error(f"Error procesando cliente {row['cli_coda']}: {e}", exc_info=True)
            continue  # Saltar al siguiente cliente en caso de error
    
    return results  # Devolver todas las respuestas acumuladas



if __name__ == "__main__":
    result = view_invoice_customer_data()
    if isinstance(result, list) and result:
         procesar_customer_data(result)
