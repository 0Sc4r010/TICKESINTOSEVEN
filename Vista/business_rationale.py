import sys
from zeep import Client
from zeep.transports import Transport
from requests import Session
from lxml import etree
from Vista.create_posting import payment_collection, prepare_ts_recaj_data
from dominus_sales_api.config import wsdl_fac
from modelos.data_access import  find_pettycash, receivable_consultation, updated_status, valida_no_registro, view_invoice_data_head
import logging

# Configuración del cliente SOAP
def get_soap_client(wsdl_url=wsdl_fac):
    session = Session()
    session.verify = False  # Cambiar a True si tienes un certificado SSL válido
    transport = Transport(session=session)
    return Client(wsdl=wsdl_url, transport=transport)

client = get_soap_client()

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("app.log"),
                        logging.StreamHandler()
                    ])

logger = logging.getLogger(__name__)

def insertar_recuado(vDetalle, fecha_recaudo, cfl_codi,fac_cref):
    toTsRecaj_data = prepare_ts_recaj_data(vDetalle,fecha_recaudo,cfl_codi,fac_cref )
    if toTsRecaj_data:  #isinstance(toTsRecaj_data, list) and
        response = payment_collection(toTsRecaj_data)
    return response

# Función para analizar la respuesta XML y extraer el valor de FAC_CONT
def extraer_fac_cont(parametro, respuesta_xml):
    try:
        parametro = parametro.strip("<>")
        if not parametro.startswith(".//"):
            parametro = f".//{parametro}"
        root = etree.fromstring(respuesta_xml)
        fac_cont = root.find(parametro).text
        return int(fac_cont)
    except Exception as e:
        logger.error("Error al analizar el XML:", e)
        return None


'''metodo para getionar el valor a pagar'''
def create_vfopa(detalle):
    total_pagar = 0  # Inicializa la variable antes del bucle
    for vcadic in detalle:
        if 'dfa_civa' in vcadic:  # Verifica si el campo 'dfa_civa' existe
            total_pagar += vcadic['dfa_cant'] * vcadic['dfa_civa']
        else:
            total_pagar += vcadic['dfa_cant'] * vcadic['dfa_valo'] 
        
    try:    
        return {
            'TSFaDfopa': [
                {
                    'Fop_codi' : 101,
                    'Tac_codi' : 0, 
                    'Dfo_nume' : '', 
                    'Dfo_valo' : total_pagar,
                    'Dfo_fech' : detalle[0]['fac_fech'],
                    'Dfo_comp' : 'S',
                    'Caj_codi' : find_pettycash(detalle[0]['id_proyecto'], detalle[0]['operacion']),  
                    'Dfo_desc' : 'recaudo automatico',
                    'Ban_codi' : 0,
                    'Dfo_chec' : 0 
                }
            ]
        }
    except Exception as e:
        logger.error(f"Error processing invoice: {e}", exc_info=True)





def calcula_factura_no_tasa(detalle):
    try:
        total_brutofac = 0  # Inicializa la variable antes del bucle 
        for vbrfac in detalle:
            total_brutofac += vbrfac['dfa_cant'] * vbrfac['dfa_valo']
        return total_brutofac
        
    except Exception as e:
        logger.error(f"Error processing invoice: {e}", exc_info=True)   
    
    
    

def create_vDistribA(detalle):
    try:
        return {
            'TSFaDdisp': [
                {'Tar_codi': 1, 'Arb_codi': detalle[0]['area'], 'Ddi_tipo': 'P', 'Ddi_valo': 0, 'Ddi_porc': 100},
                {'Tar_codi': 4, 'Arb_codi': detalle[0]['proyecto'], 'Ddi_tipo': 'P', 'Ddi_valo': 0, 'Ddi_porc': 100},
                {'Tar_codi': 2, 'Arb_codi': detalle[0]['sucursal'], 'Ddi_tipo': 'P', 'Ddi_valo': 0, 'Ddi_porc': 100},
                {'Tar_codi': 3, 'Arb_codi': detalle[0]['ctro_Costo'], 'Ddi_tipo': 'P', 'Ddi_valo': 0, 'Ddi_porc': 100}
            ]
        }
    except Exception as e:
        logger.error(f"Error processing invoice: {e}", exc_info=True)   



def insertar_detalles(detalles, vDistribA):
    return {
        'TSFaDfact': [
            {
                'Bod_codi': detalle['bod_codi'],
                'Pro_codi': detalle['pro_codi'],
                'Uni_codi': detalle['uni_codi'],
                'Dfa_cant': detalle['dfa_cant'],
                'Dfa_valo': detalle['dfa_valo'],
                'Dfa_tide': detalle['dfa_tide'],
                'Dfa_pvde': round(detalle['dfa_pvde'], 2),
                'Dfa_desc': detalle['dfa_desc'],
                'Dfa_dest': detalle['destino'],
                'Ctr_cont': 0,
                'Tip_codi': 0,
                'Cli_coda': detalle['cli_coda'],
                'Dcl_codd': 1,
                'Lot_fven': '2029-07-30',
                'vDistribA': vDistribA
            } for detalle in detalles
        ]
    }

def insertar_encabezado_fc(row,proceso_global):
    try:
        if proceso_global in ('T', 'R'):
            detalles_cursor = view_invoice_data_head(proceso_global, 'D', 0, row['fac_nume'],'N')
        else:    
            detalles_cursor = view_invoice_data_head(proceso_global, 'I',  row['id_origen'], row['doc_nume'], row['nom_disp'])
        if isinstance( detalles_cursor, list) and  detalles_cursor:
            vDistribA = create_vDistribA(detalles_cursor)
            vDetalle = insertar_detalles(detalles_cursor, vDistribA)
            vFopa = create_vfopa(detalles_cursor)
            # vCAdic =  create_vcadic(detalles_cursor) 
            factura = {
                'Emp_codi': row['id_proyecto'],
                'Top_codi': row['operacion'],
                'Fac_nume': 0,
                'Fac_fech': row['fac_fech'],
                'Fac_desc': row['des_hfac'] + '-' + str(row['fac_nume']),
                'Arb_csuc': row['sucursal'],
                'Tip_codi': 0,
                'Cli_coda': row['cli_coda'],
                'Dcl_codd': row['Dcl_codd'],
                'Mon_codi': row['Mon_codi'],
                'Fac_tdis': 'A',
                'Fac_tipo': row['fac_tipo'],
                'Fac_feta': row['fac_fech'],
                'Fac_feci': row['fac_fech'],
                'Fac_fecf': row['fac_fech'],
                'Fac_cref': row['fac_cref'],
                'Fac_fepo': row['fac_fech'],
                'Fac_fepe': row['fac_fech'],
                'Fac_fext': row['fac_fech'],
                'Mco_cont': 0,
                'Fac_tido': 'M',
                'Fac_pepe': 0,
                'Fac_pext': 0,
                'Fac_peri': 0,
                'Fac_esth': 0,
                # 'Con_codi': 1,
                'Fac_esta': 'A',
                'Fac_fepf': row['fac_fech'],  
                'vDetalle': vDetalle
            }
            # "La forma de pago ha sido inhabilitada por orden del departamento financiero, con el objetivo de solucionar el redondeo de decimales,
            #  dado que ya se está realizando el cálculo de forma independiente."
                #factura['vFopa'] = vFopa

            try:
                retorno = 0
                Ctaxcobrar = receivable_consultation(row['id_proyecto'], row['fac_cref'])
                if Ctaxcobrar and row['fac_tipo'] == 'F':
                    response = insertar_recuado(detalles_cursor, row['fac_fech'], row['cfl_codi'],row['fac_cref'])
                    retorno =  response['Retorno']
                else :
                     exisfact = valida_no_registro(row['id_proyecto'], row['fac_cref'])
                     if not exisfact:
                        resultado_insercion = client.service.InsertarFactura(pFactura=factura)
                        retorno = extraer_fac_cont('.//RETORNO', resultado_insercion) 
                        if retorno!= 0:
                            logger.info(f"error en la insercion : {resultado_insercion}", exc_info=True)
                        else:    
                            if row['fac_tipo'] == 'F': # and proceso_global =='T':
                                response = insertar_recuado(detalles_cursor, row['fac_fech'], row['cfl_codi'], row['fac_cref'])
                                retorno =  response['Retorno']
                updated_status(proceso_global, retorno, row['fac_nume'] if proceso_global == 'T' else row['fac_cref'], row['id_proyecto'])
                    
            except Exception as e:
                logger.error(f"Error processing invoice: {e}", exc_info=True)
        else:
            logger.info(f"there is an error --> No procesado: {row['doc_nume']}", exc_info=True)       
    except Exception as e:
        logger.error(f"Error al procesar el encabezado de la factura para el registro: {row}", exc_info=True)
        
if __name__ == "__main__":
    result = view_invoice_data_head()
    if isinstance(result, list) and result:
        insertar_encabezado_fc(result, branch_id=0)

