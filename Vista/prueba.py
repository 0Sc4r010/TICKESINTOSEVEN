from zeep import Client

class SOAPClient:
    def __init__(self, wsdl):
        self.client = Client(wsdl=wsdl)
    
    def anular_factura(self, emp_codi, fac_cont):
        response = self.client.service.AnularFactura(emp_codi=emp_codi, fac_cont=fac_cont)
        return response

    def insertar_factura(self, factura):
        response = self.client.service.InsertarFactura(pFactura=factura)
        return response

def get_soap_client():
    wsdl = 'http://172.18.200.72/Seven/Webservicesoap/UFaFactu/SFaFactu.asmx?WSDL'
    return SOAPClient(wsdl)

def create_vDistribA():
    return {
        'TSFaDdisp': [
            {'Tar_codi': 1, 'Arb_codi': '01', 'Ddi_tipo': 'P', 'Ddi_valo': 0, 'Ddi_porc': 100},
            {'Tar_codi': 4, 'Arb_codi': 'V888888', 'Ddi_tipo': 'P', 'Ddi_valo': 0, 'Ddi_porc': 100},
            {'Tar_codi': 2, 'Arb_codi': '10410003', 'Ddi_tipo': 'P', 'Ddi_valo': 0, 'Ddi_porc': 100},
            {'Tar_codi': 3, 'Arb_codi': '04003', 'Ddi_tipo': 'P', 'Ddi_valo': 0, 'Ddi_porc': 100}
        ]
    }

 
def create_vDetalle(vDistribA):
    return {
        'TSFaDfact': {
            'Bod_codi': '4103',
            'Pro_codi': '2010801068',
            'Uni_codi': 1,
            'Dfa_cant': 1,
            'Dfa_valo': 62500,
            'Dfa_tide': 'P',
            'Dfa_pvde': 0,
            'Dfa_desc': 'Ws',
            'Dfa_dest': 601,
            'Ctr_cont': 0,
            'Tip_codi': 0,
            'Cli_coda': '14214664',
            'Dcl_codd': 1,
            'Lot_fven': '2025-07-30',
            'vDistribA': vDistribA
        }
    }

def example_usage():
    client = get_soap_client()

    # Ejemplo de datos para AnularFactura
    emp_codi = 1
    fac_cont = 123456
    resultado_anulacion = client.anular_factura(emp_codi, fac_cont)
    print(f'Resultado de AnularFactura: {resultado_anulacion}')

    # Ejemplo de datos para InsertarFactura
    vDistribA = create_vDistribA()
    vDetalle = create_vDetalle(vDistribA)
    
    factura = {
        'Emp_codi': 1,
        'Top_codi': 3468,
        'Fac_nume': 0,
        'Fac_fech': '2024-07-30',
        'Fac_desc': 'Ws',
        'Arb_csuc': '10410003',
        'Tip_codi': 0,
        'Cli_coda': '14214664',
        'Dcl_codd': 1,
        'Mon_codi': 1,
        'Fac_tdis': 'A',
        'Fac_tipo': 'F',
        'Fac_feta': '2024-07-30',
        'Fac_feci': '2024-07-30',
        'Fac_fecf': '2024-07-30',
        'Fac_cref': '000',
        'Fac_fepo': '2024-07-30',
        'Fac_fepe': '2024-07-30',
        'Fac_fext': '2024-07-30',
        'Mco_cont': 0,
        'Fac_tido': 'M',
        'Fac_pepe': 0,
        'Fac_pext': 0,
        'Fac_peri': 0,
        'Fac_esth': 0,
        'Fac_esta': 'A',
        'Fac_fepf': '2024-07-30',
        'vDetalle': vDetalle
    }

    resultado_insercion = client.service.insertar_factura(factura)
    print(f'Resultado de InsertarFactura: {resultado_insercion}')

if __name__ == '__main__':
    example_usage()
