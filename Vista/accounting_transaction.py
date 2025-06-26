import sys
from zeep import Client
from zeep.transports import Transport
from requests import Session
from lxml import etree
from dominus_sales_api.config import wsdl_acc
from modelos.data_access import  updated_status, view_invoice_data_head,build_compr
import logging

# Configuración del cliente SOAP
def get_soap_client(wsdl_url=wsdl_acc):
    session = Session()
    session.verify = False  
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


# Función para analizar la respuesta XML y extraer el valor de FAC_CONT
def extraer_fac_cont(parametro, respuesta_xml):
    try:
        root = etree.fromstring(respuesta_xml)
        fac_cont = root.find(parametro).text
        return int(fac_cont)
    except Exception as e:
        logger.error("Error al analizar el XML:", e)
        return None


def insertar_detalles(detalles_cursor,branch_id,frmpago,Pcosto = 0):
    TSCnDMCON = []  # Inicializa una lista vacía para almacenar los registros
    for detalle in detalles_cursor:
        contaliliza = build_compr(0, detalle['id_proyecto'], branch_id, detalle['operacion'],  detalle['pro_codi'], detalle['dfa_cant'] * detalle['dfa_valo'], detalle['cli_coda'],
                                   f"Prd.{detalle['pro_codi']} -vta- {detalle['dfa_cant']}" , detalle['ctro_Costo'], detalle['proyecto'], detalle['area'], detalle['sucursal'],frmpago,
                                   Pcosto)
        for registro in contaliliza:
            TSCnDMCON.append({
                'Cue_codi': registro['cue_codi'],
                'Dmc_desc': registro['dmc_desc'],
                'Dmc_cant': detalle['dfa_cant'],  #   registro['dmc_cant'],
                'Dmc_acti': registro['dmc_acti'],
                'Dmc_refe': registro['dmc_refe'],
                'Dmc_vadb': registro['dmc_vadb'],
                'Dmc_vacr': registro['dmc_vacr'],
                'Dmc_vaba': registro['dmc_vaba'],
                'Ter_coda': registro['ter_coda'],
                'Arb_codc': registro['arb_codc'],
                'Arb_codp': registro['arb_codp'],
                'Arb_coda': registro['arb_coda'],                                                   
                'Arb_cods': registro['arb_csuc'],
                'Ter_codm': 0
            })
    return {'TSCnDmcon': TSCnDMCON}  # Retorna la lista con todos los registros insertados
        
        
 
def insertar_encabezado_mc(row,  proceso_global,  Pcosto = 0,):
    vlr_costo = Pcosto
    detalles_cursor = view_invoice_data_head(proceso_global, 'I', row['id_origen'], row['doc_nume'],row['nom_disp'])
    vDetalle = insertar_detalles(detalles_cursor,row['id_origen'],row['payment'],  vlr_costo )
    Contable = {
            'Emp_codi':row['id_proyecto'],
            'Top_codi':row['opr_cntb'],
            'Mco_nume':row['fac_nume'],
            'Mco_fech':row['fac_fech'],
            'Mod_codi':0,
            'Arb_csuc':row['sucursal'],
            'Mco_desc':f"{row['des_hfac']} -Fac- {row['fac_nume']}",
            'vDetalle': vDetalle 
        }
    resultado_insercion = client.service.InsertarMovContable(pContable=Contable)
    if extraer_fac_cont('.//RETORNO', resultado_insercion) != 0:
        logger.info(f"there is an error -->: {resultado_insercion}, {row['doc_nume']}, {row['nom_disp']}", exc_info=True)
    else:    
        logger.info(f"processed -->: {resultado_insercion}", exc_info=True)
        
    updated_status(proceso_global, extraer_fac_cont('.//RETORNO', resultado_insercion), row['doc_nume'], row['id_proyecto'])  
    
if __name__ == "__main__":
    result = view_invoice_data_head()
    if isinstance(result, list) and result:
        insertar_encabezado_mc(result, branch_id=0)

