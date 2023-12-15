
using Grpc.Core;
using MySql.Data.MySqlClient;
using MySqlX.XDevAPI.Relational;

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.OleDb;

using System.IO;
using System.Xml;
using System.Data.Common;
using Newtonsoft.Json; 

namespace siigo 
{
    public class ConnetionDataBase
    {
        /* this.Url = "";
            this.UserDataBase = "sa";
            this.PassDataBase = "123456789";
            this.NameDataBase = "Africanas";*/
        public Config config = Config.GetInstance();
        private string ConnectionString;
        private OleDbConnection conn = null;
        private static ConnetionDataBase _instance;
        private SqlConnection connAux = null;
        private StreamWriter file;
        public static readonly Guid Primary_Keys; 
        private ConnetionDataBase() {
            ConnectionString = this.config.GetAccessConnetionString();
            this.conn = new OleDbConnection(ConnectionString);
            this.conn.Open();

            //ConnectionString = this.config.GetConnetionStringAux();
            //this.connAux = new MySqlConnection(ConnectionString);
            //this.connAux.Open();
            ConnectionString = this.config.GetConnetionString();
            this.connAux = new SqlConnection(ConnectionString);
            this.connAux.Open();


        }
        public string CadenaFormateada(string cadena)
        {
            string puntos = "/*::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/";
            string cadenaFinal;
            cadenaFinal = puntos.Remove(6) + cadena.Replace(" ", ":") +  puntos.Substring(cadena.Length);
            return cadenaFinal;

        }
        public static List<string> getKeyNames(String tableName, DbConnection conn)
        {
            var returnList = new List<string>();


            DataTable mySchema = (conn as OleDbConnection).
                GetOleDbSchemaTable(OleDbSchemaGuid.Primary_Keys,
                                    new Object[] { null, null, tableName });


            // following is a lengthy form of the number '3' :-)
            int columnOrdinalForName = mySchema.Columns["COLUMN_NAME"].Ordinal;

            foreach (DataRow r in mySchema.Rows)
            {
                returnList.Add(r.ItemArray[columnOrdinalForName].ToString());
            }

            return returnList;
        }

        public static ConnetionDataBase GetInstance()
        {
            if (_instance == null)
            {
                _instance = new ConnetionDataBase();
            }
            return _instance;
        }

        public void ExecuteQueries(string Query_)
        {
            if (Query_.Trim() != "") {
                Console.WriteLine(Query_);  
              OleDbCommand cmd = new OleDbCommand(Query_, this.conn);
              cmd.ExecuteNonQuery();
            }
        }
        public void ExecuteSQLQueries(string Query_)
        {
            if (Query_.Trim() != "")
            {
                SqlCommand cmd = new SqlCommand(Query_, this.connAux);
                cmd.ExecuteNonQuery();
            }
        }
        //public void ExecuteSQLQueries(string Query_)
        //{
        //    Console.Write('*');
        //    if (Query_.Trim() != "")
        //    {
        //        MySqlCommand cmd = new MySqlCommand(Query_.Trim(), this.connAux);
        //        cmd.ExecuteNonQuery();
        //    }
        //}
        public OleDbDataReader DataReader(string Tabla, string[] Columnas = null, string[][] WhereA = null, string[] ColOrder = null)
        {
            string ColumString = "";
            string ColumOrderString = "";
            string WhereString = "";



            if (ColOrder == null) { ColumOrderString = ""; }
            else if (ColOrder.Length <= 0) { ColumOrderString = ""; } else {
                string coma = "";
                foreach (string col in ColOrder) {
                    ColumOrderString += coma + "[" + col + "]";
                    coma = ", ";
                }

            }
            if (ColumOrderString.Trim() != "")
            {
                ColumOrderString = " order by " + ColumOrderString;
            }
            if (Columnas == null) { ColumString = "*"; }
            else if (Columnas.Length <= 0) { ColumString = "*"; }
            else {
                string coma = "";
                foreach (string col in Columnas)
                {
                    ColumString += coma + col;
                    coma = ", ";
                }

            }

            if (WhereA != null && WhereA.Length > 0) {
                string And = "";
                string relacion = "";

                for (int i = 0; i < WhereA.Length; i++)
                {
                    string[] Where = WhereA[i];

                    if (Where[1] != "")

                        switch (Where[1].ToUpper()) {
                            case "LIKE": relacion = " like '%" + Where[2] + "%' ";
                                break;
                            case "IN":
                                relacion = " in  (" + Where[2] + " ) ";
                                break;
                            case "NOTIN":
                                relacion = " not in  (" + Where[2] + " ) ";
                                break;

                            case "BW":
                                relacion = " between  '" + Where[2] + "' and '" + Where[3] + "' ";
                                break;
                            case "IT":
                                relacion = " =  '" + Where[2] + "' ";
                                break;
                            case "LT":
                                relacion = " <  '" + Where[2] + "' ";
                                break;
                            case "MT":
                                relacion = " >  '" + Where[2] + "' ";
                                break;
                            case "LET":
                                relacion = " <=  '" + Where[2] + "' ";
                                break;
                            case "MET":
                                relacion = " >=  '" + Where[2].Trim() + "' ";
                                break;
                        }

                    WhereString += And + "  " + Where[0] + " " + relacion;
                    And = "and ";
                }
                if (WhereString.Trim() != "") {
                    WhereString = " Where " + WhereString;
                }

            }
            string Query_ = "select " + ColumString + " from " + Tabla.Trim() + "  " + WhereString + ColumOrderString;
           // Console.WriteLine(Query_);
            OleDbCommand cmd = new OleDbCommand(Query_, this.conn);
            OleDbDataReader dr = cmd.ExecuteReader();

            return dr;
        }
        public SqlDataReader DataReaderAux(string Tabla, string[] Columnas = null, string[][] WhereA = null)
        {
            string ColumString = "";
            string WhereString = "";
            if (Columnas == null) { ColumString = "*"; }
            else if (Columnas.Length <= 0) { ColumString = "*"; }
            else
            {
                string coma = "";
                foreach (string col in Columnas)
                {
                    ColumString += coma + " " + col + " ";
                    coma = ", ";
                }

            }


            if (WhereA != null && WhereA.Length > 0)
            {
                string And = "";
                string relacion = "";

                for (int i = 0; i < WhereA.Length; i++)
                {
                    string[] Where = WhereA[i];

                    if (Where[1] != "")

                        switch (Where[1].ToUpper())
                        {
                            case "LIKE":
                                relacion = " like '%" + Where[2] + "%' ";
                                break;
                            case "IN":
                                relacion = " in  (" + Where[2] + " ) ";
                                break;
                            case "NOTIN":
                                relacion = " not in  (" + Where[2] + " ) ";
                                break;

                            case "BW":
                                relacion = " between  '" + Where[2] + "' and '" + Where[3] + "' ";
                                break;
                            case "IT":
                                relacion = " =  '" + Where[2] + "' ";
                                break;
                            case "LT":
                                relacion = " <  '" + Where[2] + "' ";
                                break;
                            case "MT":
                                relacion = " >  '" + Where[2] + "' ";
                                break;
                            case "LET":
                                relacion = " <=  '" + Where[2] + "' ";
                                break;
                            case "MET":
                                relacion = " >=  '" + Where[2].Trim() + "' ";
                                break;
                        }

                    WhereString += And + "  " + Where[0] + " " + relacion;
                    And = "and ";
                }
                if (WhereString.Trim() != "")
                {
                    WhereString = " Where " + WhereString;
                }

            }

            string Query_ = "select " + ColumString + " from " + Tabla.Trim() + "  " + WhereString;
            SqlCommand cmd = new SqlCommand(Query_, this.connAux);
            SqlDataReader dr = cmd.ExecuteReader();
            return dr;
        } 
        public DataTable GetTablasConnPrincipal() {
            DataTable tables = conn.GetSchema("Tables");
            return tables;
        }
        public DataTable GetColumnasConnPrincipal()
        {
            DataTable columnas = conn.GetSchema("Columns");
            return columnas;
        }

        public DataTable GetDataTypesConnPrincipal()
        {
            DataTable DataTypes = conn.GetSchema("DataTypes");
            return DataTypes;
        }

        public DataTable GetColumnasConnPrincipal(string Tabla)
        {
            DataTable columnas = conn.GetSchema("Columns", new [] { conn.Database, null, Tabla });
            return columnas;
        }

        public DataTable GetConstrainConnPrincipal()
        {
            DataTable columnas = conn.GetSchema("Restrictions");
            return columnas;
        }


        public List<TablaPrincipal> CargartablasOrigenTabla( )  {
            try {
            List<TablaPrincipal> keys = new List<TablaPrincipal>();
            TablaPrincipal obj = null; 
            DataTable tablas = GetTablasConnPrincipal(); 
            DataTable TablaColumnas = GetColumnasConnPrincipal();
            DataTable dtDataTypes = GetDataTypesConnPrincipal();

                foreach (DataRow tabla in tablas.Rows) {
                  if (!tabla["TABLE_NAME"].ToString().Contains("MSys"))
                        { 
                    List<string> primarys = getKeyNames(tabla["TABLE_NAME"].ToString(), conn);
                        List<string> primaria = new List<string>() ;
                    if (primarys.Count > 0) {
                        Console.WriteLine("claves primaria : " + primarys.ToString());
                        primaria = primarys ;
                    }
                    obj = new TablaPrincipal(tabla["TABLE_NAME"].ToString(), primaria);


                    DataRow[] dr_columnas = TablaColumnas.Select("TABLE_NAME = '" + tabla["TABLE_NAME"].ToString() + "'");

                    // scriptSql.WriteLine("tabla : " + tabla["TABLE_NAME"].ToString());
                    foreach (DataRow columnas in dr_columnas)
                    {

                        ColumDataBase col = new ColumDataBase();
                        if (columnas["TABLE_NAME"].ToString() == tabla["TABLE_NAME"].ToString()) {

                            // scriptSql.WriteLine("columna : " + columnas["COLUMN_NAME"].ToString());
                            col.Name = columnas["COLUMN_NAME"].ToString();

                            DataRow[] drDataTypes = dtDataTypes.Select("NativeDataType = '" + columnas["DATA_TYPE"].ToString() + "'");

                            foreach (DataRow type in drDataTypes)
                            { col.DATA_TYPE = type["TypeName"].ToString(); }

                            if (columnas["IS_NULLABLE"] != null)
                                col.IS_NULLABLE = Boolean.Parse(columnas["IS_NULLABLE"].ToString());
                            if (columnas["CHARACTER_MAXIMUM_LENGTH"].ToString().Trim() != "")
                                col.CHARACTER_MAXIMUM_LENGTH = int.Parse(columnas["CHARACTER_MAXIMUM_LENGTH"].ToString());
                            if (columnas["NUMERIC_PRECISION"].ToString().Trim() != "")
                                col.NUMERIC_PRECISION = int.Parse(columnas["NUMERIC_PRECISION"].ToString());
                            if (columnas["NUMERIC_SCALE"].ToString().Trim() != "")
                                col.NUMERIC_SCALE = int.Parse(columnas["NUMERIC_SCALE"].ToString());
                            if (obj.primaryKey.Contains(col.Name)   ) {
                               //
                               //col.IS_PRIMARY = true;
                                if (col.NUMERIC_PRECISION > 0 && obj.primaryKey.Count == 1)
                                {
                                    //col.AUTO_INCREMENT = true;
                                }
                            }
                        }
                        obj.columDataBase.Add(col);
                    } 
                    keys.Add(obj);
                }}          
               // string json = JsonConvert.SerializeObject(obj, Newtonsoft.Json.Formatting.Indented);
               // scriptSql.WriteLine(json); 
                return keys;
            }
            catch (Exception ex) { throw new Exception("CargartablasOrigenTabla  => " + ex.ToString()); }

        }
        public Boolean  ValidaTablaMysql(string tablaValidacion) {
            try { 
            SqlDataReader dr_tablas_exist_aux;
            string[][] where_ = new string[][]
               { new string[] { "TABLE_NAME", "IT", tablaValidacion  } };
            dr_tablas_exist_aux = this.DataReaderAux("information_schema.TABLES", new string[] { "COUNT(TABLE_NAME) as existe" }, where_);
            Console.WriteLine(":::::::VALIDANDO:TABLA::"+ tablaValidacion +  ":::::::::::::::::::::::");
            dr_tablas_exist_aux.Read();
            bool existe = (0 == int.Parse(dr_tablas_exist_aux["existe"].ToString())) != true; 
            dr_tablas_exist_aux.Close(); 
            return existe;
            }
            catch (Exception ex) { throw new Exception("ValidaTablaMysql : " + tablaValidacion + " - error =>" + ex.ToString()); }
        }
        public bool ValidaTablaSqlServer(string tablaValidacion)
{
    try
    {
        SqlDataReader dr_tablas_exist_aux;
        string[][] where_ = new string[][]
        {
            new string[] { "TABLE_NAME", "IT", tablaValidacion } 
        };
                 

        dr_tablas_exist_aux = this.DataReaderAux(
            "INFORMATION_SCHEMA.TABLES",
            new string[] { "COUNT(TABLE_NAME) as existe" },
            where_ 
        );

        Console.Write(CadenaFormateada($"VALIDANDO TABLA {tablaValidacion}"));
        dr_tablas_exist_aux.Read();
        bool existe = int.Parse(dr_tablas_exist_aux["existe"].ToString()) > 0;
        dr_tablas_exist_aux.Close();
                Console.WriteLine($"Existe:en:destino=>{existe}::::");
                return existe;
    }
    catch (Exception ex)
    {
        throw new Exception("ValidaTablaSqlServer : " + tablaValidacion + " - error =>" + ex.ToString());
    }
}


        public OleDbDataReader GetColumnasTabla(string NameTable) {
            string[][] where_ = new string[][]
              { new string[] { "a.TABLE_NAME", "IT", NameTable } };

            string[] columnas_ = new string[] { "a.*", "case when coalesce( b.[COLUMN_NAME] , '' ) != ''  then 'YES' ELSE 'NOT' END AS IS_PRIMARY" };

            string tablaInfo_ = "information_schema.columns a left join [dbo].[tabla_primary_key] b  on a.[TABLE_NAME] = b.[TABLE_NAME] and a.[COLUMN_NAME] = b.[COLUMN_NAME] ";

            return this.DataReader(tablaInfo_, columnas_, where_);
        }

 
        public void ExecuteNonQueryMysql(string Query_) {
            try
            {
                SqlCommand cmd = new SqlCommand(Query_, this.connAux);
                cmd.ExecuteNonQuery(); 
            }
            catch (Exception e)
            { throw new Exception(e.Message); }
        }


        public object ShowDataInGridView(string Query_)
        {
            OleDbDataAdapter dr = new OleDbDataAdapter(Query_, this.conn);
            DataSet ds = new DataSet();
            dr.Fill(ds);
            object dataum = ds.Tables[0];
            return dataum;
        }
        public void CloseConnection()
        {
           this.conn.Close();
            this.connAux.Close();
        }



    }
}
