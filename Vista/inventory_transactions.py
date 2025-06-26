import sys
from zeep import Client
from zeep.transports import Transport
from requests import Session
from lxml import etree
from dominus_sales_api.config import wsdl_inv
import logging
from modelos.data_access import  view_invoice_data_head
from Vista.accounting_transaction import extraer_fac_cont
import logging

# Configuración del cliente SOAP
def get_soap_client(wsdl_url=wsdl_inv):
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


def create_vDistribA(detalle):
    return {
        'TSInDdisp': [
            {'Tar_codi': 1, 'Arb_codi': detalle[0]['area'], 'Ddi_tipo': 'P', 'Ddi_valo': 0, 'Ddi_porc': 100},
            {'Tar_codi': 4, 'Arb_codi': detalle[0]['proyecto'], 'Ddi_tipo': 'P', 'Ddi_valo': 0, 'Ddi_porc': 100},
            {'Tar_codi': 2, 'Arb_codi': detalle[0]['sucursal'], 'Ddi_tipo': 'P', 'Ddi_valo': 0, 'Ddi_porc': 100},
            {'Tar_codi': 3, 'Arb_codi': detalle[0]['ctro_Costo'], 'Ddi_tipo': 'P', 'Ddi_valo': 0, 'Ddi_porc': 100}
        ]
    }


def insertar_detalles(detalles_cursor,vDistribA):
    TSInDminv = []  # Inicializa una lista vacía para almacenar los registros
    for detalle in detalles_cursor:
        TSInDminv.append(
            {
                'Pro_codi': detalle['pro_codi'],
                'Uni_codi': detalle['uni_codi'],
                'Bod_codi': detalle['bod_codi'],
                'Bod_dest': 0,
                'Dmi_desc': detalle['dfa_desc'],
                'Dmi_cant': detalle['dfa_cant'],
                'Dmi_valo': detalle['dfa_valo'],
                'Dmi_dest': detalle['destino'], 
                'Dmi_ajus': 0,
                'Lot_fven': '9999-07-30',
                'Lot_codi': '', 
                'Ctr_cont': 0,
                'Ctr_dcon': 0,
                'vDistribA': vDistribA
            })
    return {'TSInDminv': TSInDminv}  # Retorna la lista con todos los registros insertados
        
        
 
def insertar_encabezado_in(row,proceso_global):
    detalles_cursor = view_invoice_data_head(proceso_global, 'I', row['id_origen'], row['doc_nume'],row['nom_disp'])
    if isinstance( detalles_cursor, list) and  detalles_cursor:
        vDistribA = create_vDistribA(detalles_cursor)
        vDetalle = insertar_detalles(detalles_cursor,vDistribA)
        Inventario = {
            'Emp_codi':row['id_proyecto'],
            'Top_codi':row['opr_inve'],
            'Min_nume':row['fac_nume'],
            'Min_fech':row['fac_fech'],
            'Min_desc':row['des_hfac'],
            'Min_esta':'A',
            'Arb_csuc':row['sucursal'],
            'Ter_coda':row['cli_coda'],
            'Ter_codr':0,
            'Min_feac':row['fac_fech'],
            'Min_nuac':0,
            'Min_tdis':'A',
            'Pla_codi':0,
            'Min_orig':'F',
            'Top_fact':0,
            'Fac_pref' : '',
            'Fac_nfap' : '',
            'vDetalle': vDetalle
        }
        resultado_insercion = client.service.InsertarMovimiento(pMovInve=Inventario)
        if extraer_fac_cont('.//RETORNO', resultado_insercion) != 0:
            logger.info(f"there is an error -->: {resultado_insercion}", exc_info=True)
        else:    
            logger.info(f"processed -->: {resultado_insercion}", exc_info=True)
            return extraer_fac_cont('.//MIN_CONT', resultado_insercion)
    else:
        logger.info(f"There is an error --> doc_nume: {row['doc_nume']}, id_origen: {row['id_origen']}", exc_info=True)


      
if __name__ == "__main__":
    result = view_invoice_data_head()
    if isinstance(result, list) and result:
        insertar_encabezado_in(result, branch_id=0)