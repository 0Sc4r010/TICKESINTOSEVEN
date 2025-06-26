import time
import pymssql
import logging

# Configuración del logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("app.log"),
                        logging.StreamHandler()
                    ])

logger = logging.getLogger(__name__)

class MSSQLConnectionManager:
    _connection = None
 
    @staticmethod
    def get_connection():
        if MSSQLConnectionManager._connection is None :
           try:
            # Credenciales de conexión a la Base
              MSSQLConnectionManager._connection = pymssql.connect(server=r'172.18.200.14',user='seven',password="Gav:[Z3A@X7", database='seven', as_dict=True)  # Production
              # MSSQLConnectionManager._connection = pymssql.connect(server=r'172.18.200.14\pruebas',user='seven', password = 'seven', database='seven_pruebas', as_dict=True) # testing
           except pymssql.DatabaseError as e:
              MSSQLConnectionManager._connection = None
              print(f"Error connecting to database: {e}")
        return MSSQLConnectionManager._connection
            
    @staticmethod
    def close_connection():
        """
        Cierra la conexión activa a la base de datos si existe.
        """
        if MSSQLConnectionManager._connection:
            try:
                MSSQLConnectionManager._connection.close()
                MSSQLConnectionManager._connection = None
            except pymssql.DatabaseError as e:
                print(f"Error closing database connection: {e}")
    

        
def execute_query(query, params=None,fetch=True): 
    conn = MSSQLConnectionManager.get_connection()
    if conn:
        try:
            with conn.cursor(as_dict=True) as cur:
                if params:
                    cur.execute(query, params)
                else:
                    cur.execute(query)
                if fetch:
                    return cur.fetchall()  
                else: 
                    conn.commit()  
                    return None  
        except pymssql.DatabaseError as e:
            print(f"Error executing query: {e}")
            return None
        except Exception as e:
            # Capturar cualquier otro error
            print(f"Otro error ha ocurrido: {e}")
            return None
    else:
        print("Failed to establish database connection.")
        return None


def iif(condicion, valor_si_verdadero, valor_si_falso):
    return valor_si_verdadero if condicion else valor_si_falso


def process_tickes(fecha):
    """
    The function `process_tickets` executes a stored procedure `sp_GetFacturacionElectronica` with a
    given date parameter to retrieve electronic billing information.
    
    :param fecha: The `process_tickets` function seems to be a Python function that is intended to
    execute a stored procedure `sp_GetFacturacionElectronica` with a parameter `@FechaFactura` being
    passed as input. The function then calls `execute_query` with the query and parameters to execute
    the stored
    :return: The function `process_tickes` is returning the result of executing a query using the
    provided `fecha` parameter as the `@FechaFactura` value in a stored procedure
    `sp_GetFacturacionElectronica`. The query is executed with the `fetch=False` parameter, which
    indicates that the query result should not be fetched immediately.
    """
    query = "exec sp_GetFacturacionElectronica @FechaFactura=%s"
    params = (fecha )
    return execute_query(query,params,fetch=False)    


def build_compr(fac_cont,emp_codi,branch_id,operacion,producto,valor,tercero,descrip,arb_codc,arb_codp,arb_coda,arb_cods,frmpago,Pcosto):
    query = "exec  usp_accounting_transaction @mco_cont = %s, @emp_codi = %s, @id_origen = %s, @operacion = %s,@producto = %s, @valor = %s,@tercero = %s,@descrip  = %s, @arb_codc = %s,@arb_codp = %s,@arb_coda = %s,@arb_cods = %s, @frm_pago = %s, @cst_prmd = %s"
    params = (fac_cont,emp_codi,branch_id,operacion,producto,valor,tercero,descrip,arb_codc,arb_codp,arb_coda,arb_cods,frmpago,Pcosto)
    return execute_query(query,params)    

def view_invoice_data_head(proceso,tipo,branch_id = 0,numero=0,surtidor=''):
    """
    The function `view_invoice_data_head` executes a SQL query to retrieve invoice data based on
    specified parameters.
    
    :param proceso: The `proceso` parameter is used to specify the process for which you want to view
    invoice data. It is likely a code or identifier for a specific business process or operation
    :param tipo: The parameter "tipo" in the function "view_invoice_data_head" is used to specify the
    type of process or operation. It is passed as an argument to the stored procedure
    "usp_ViewticketeData" to retrieve invoice data based on the specified process type
    :param branch_id: The `branch_id` parameter is used to specify the ID of the branch for which you
    want to view invoice data. It is an optional parameter with a default value of 0, which means if no
    value is provided, the function will assume branch ID 0, defaults to 0 (optional)
    :param numero: The `numero` parameter in the `view_invoice_data_head` function is used to specify
    the invoice number for which you want to view the data, defaults to 0 (optional)
    :param surtidor: The parameter "surtidor" in the function "view_invoice_data_head" seems to
    represent the distributor or supplier related to the invoice data. It is used as a filter in the SQL
    query to retrieve specific invoice data based on the distributor or supplier information
    :return: The function `view_invoice_data_head` is returning the result of executing a query with the
    provided parameters `proceso`, `tipo`, `branch_id`, `numero`, and `surtidor` using the
    `execute_query` function.
    """
    query = "exec usp_ViewticketeData @Proceso=%s, @BranchID=%s, @FacNume=%s, @distrib=%s"
    params = (tipo, branch_id, numero,surtidor)
    return execute_query(query,params)    
 
 
def view_invoice_customer_data(proceso):
    query = "exec sp_ObtenerDatosCLientes"
    return execute_query(query)    


def receivable_consultation(empresa,fac_cref):
    query = """ select cxc_nume,cxc_nech,cxc_cont,cxc_desc,cxc_cref,cxc_tota from CA_CXCOB where cxc_cref = %s and EMP_CODI = %s AND CXC_sald > 0 """
    params = (fac_cref,empresa)
    return execute_query(query,params)    

def valida_no_registro(empresa, fac_cref):
    query = """
    SELECT CASE 
             WHEN EXISTS (
                 SELECT 1 
                 FROM fa_factu 
                 WHERE emp_codi = %s 
                   AND fac_cref = %s 
                   AND fac_esta = 'A'
             ) THEN 1 
             ELSE 0 
           END AS valida
    """
    params = (empresa, fac_cref)
    result = execute_query(query, params)

    if result and result[0]['valida'] == 1:
        return True
    else:
        return False

def validate_client(empresa,clicoda):
    query = "select count(*) as valida from FA_CLIEN  where CLI_CODA = %s  and  emp_codi = %s "
    params = (clicoda, empresa)
    result = execute_query(query,params)
    for row in result :
       return  iif(row['valida'] >= 1,False,True)
 
 
def calc_csto(empresa,invcont):
    query = 'select (DMI_CANT * DMI_VALO) AS VLR_COST from IN_DMINV where EMP_CODI = %s MIN_CONT = %s'
    params = (empresa,invcont)
    result = execute_query(query,params)
    if not result:
        return None
    else: 
        return result[0]['VLR_COST']     
 
 
 
def calc_total(empresa,faccont):
    query = 'Select LIQ_VALO from FA_DVFAC where FAC_CONT = %s AND EMP_CODI = %s  AND LIQ_CODI = %s'
    params = (faccont,empresa,'TOTAL')
    result = execute_query(query,params)
    if not result:
        return None
    else: 
        return result[0]['LIQ_VALO']    
 
    
def find_pettycash(empresa,operacion):
    query = 'select caj_codi from int_pareds where id_proyecto = %s and operacion = %s'
    params = (empresa,operacion)
    result = execute_query(query,params)
    if not result:
        return None
    else: 
        return result[0]['caj_codi']    
    



def clean_data(proceso):
    try:
        sentenc = """DELETE FROM int_datatickets WHERE est_proc = 0"""
        logger.info("Limpiando datos de la tabla int_datatickets.")
        # Ejecutar la consulta
        result = execute_query(sentenc, fetch=False)
        logger.info("Limpieza de datos completada exitosamente.")
        return result
    except Exception as e:
        logger.error(f"Error ejecutando limpieza: {e}", exc_info=True)
        return None
    

def  updated_status(proceso,estado,factura, empresa):
   sentenc = """update int_datatickets set Est_proc = %s where fac_nume = %s and emp_codi = %s"""
   params = (estado,factura, empresa)
   return execute_query(sentenc,params,fetch=False)   



if __name__ == "__main__":
 #   print(view_invoice_data_head())
    print(view_invoice_customer_data())



