using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace siigo 
{
    public class Factura
     {
        private string Prefijo;
        private int NumFactura;
        private ConnetionDataBase _conn;
        public Factura( string Prefijo , int NumFactura)
        {   this.Prefijo = Prefijo;
            this.NumFactura = NumFactura;

        }

        public void Listar()
        {
            if (this.NumFactura <= 0 || this.Prefijo.Trim() == "") {
                throw new Exception("Error al intentar listar la factura - No existen datos ");
            }
            

            this._conn = ConnetionDataBase.GetInstance();
            string[][] whereFactura_ = new string[][]
             { new string[] { "nofactura", "IT", this.NumFactura.ToString() },
                   new string[] { "trim(prefijo)", "IT", this.Prefijo.ToString() } };

            string[][] whereDescripcion_ = new string[][]
              { new string[] { "factura", "IT", this.NumFactura.ToString()  },
                   new string[] { "trim(prefijo)", "IT", this.Prefijo.ToString() } };


            OleDbDataReader dr = _conn.DataReader("facturas" , null , whereFactura_);
            dr.Read();
            Console.WriteLine("factura : " + dr["prefijo"].ToString().Trim() + "-" + dr["nofactura"].ToString());

          

            OleDbDataReader dr_detail = _conn.DataReader("DetallesFactura", null, whereDescripcion_);
            Console.WriteLine("[id] ,[factura] ,[prefijo] ,[cantidad] ,[codigo] ,[articulo] ,[PrecioVenta] ,[IVA] ,[ValorIva] ,[Total] ,[Pventa_u] ,[ValorIvaU] ,[PrecioU] ,[Descu_Por] ,[Descu_Valor] ,[UM] ,[Categoria]");
            while (dr_detail.Read())
            {

                Console.WriteLine(dr_detail["id"].ToString() + "  " + dr_detail["factura"].ToString() + "  " + dr_detail["prefijo"].ToString() + "  " + dr_detail["cantidad"].ToString() + "  " + dr_detail["codigo"].ToString() + "  " + dr_detail["articulo"].ToString() + "  " + dr_detail["PrecioVenta"].ToString() + "  " + dr_detail["IVA"].ToString() + "  " + dr_detail["ValorIva"].ToString() + "  " + dr_detail["Total"].ToString() + "  " + dr_detail["Pventa_u"].ToString() + "  " + dr_detail["ValorIvaU"].ToString() + "  " + dr_detail["PrecioU"].ToString() + "  " + dr_detail["Descu_Por"].ToString() + "  " + dr_detail["Descu_Valor"].ToString() + "  " + dr_detail["UM"].ToString() + "  " + dr_detail["Categoria"]);

            }

        }



     }
}
