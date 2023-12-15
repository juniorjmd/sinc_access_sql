using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using MySqlX.XDevAPI.Common;
using MySqlX.XDevAPI.Relational;

namespace siigo
{
    public class Sincronizacion
    {
        private readonly string puntos = "/*::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::*/";
        private Config config = Config.GetInstance();
        private ConnetionDataBase _conn = null;

        private List<TablaPrincipal> tablasOrigen = new List<TablaPrincipal>(); 


        ScriptSql scriptSql = null; 
        List<ValidacionTabla> _tablasValidacion = new List<ValidacionTabla>();
        public Sincronizacion() {
            this._conn = ConnetionDataBase.GetInstance();
        }
        public void Start()
        {
            GenerarArchivoResultado();
            if (ValidarTablasDestino())
            {
                if (!PasarDatosATablasDestino())
                {
                    Console.WriteLine("PasarDatosATablasDestino - Error en el proceso de migracion");
                }
                else {
                    try
                    {
                        GenerarMigracion();

                        AplicarMigracion();
                    }
                    catch (Exception ex) { Console.WriteLine(ex.ToString()); }
                }
            }
            else {
                Console.WriteLine("ValidarTablasDestino - Error en el proceso de migracion");
            }
        }
        private void GenerarMigracion() {
            try {
           
                string folder = scriptSql.name;
                string path =  scriptSql.path;
                string extention = scriptSql.extention; 
                int size = this._tablasValidacion.Count; 
            this._tablasValidacion.ForEach(a =>
            { size += a._queryInsertDatosDestino?.Count ?? 0; });
            this._tablasValidacion.ForEach(a =>
            {
               
                if (size > 1000) { 
                    scriptSql.CrearNuevoArchivo(a.Name, path, extention, "", folder);
                }
                scriptSql.WriteLine(CadenaFormateada("Generando Migracion En Archivo"));
                scriptSql.WriteLine(CadenaFormateada("::"));
                scriptSql.WriteLine(CadenaFormateada("TABLA  =>  " + a.Name));
                scriptSql.WriteLine(CadenaFormateada("::"));
                scriptSql.WriteLine(CadenaFormateada("EXISTE EN DESTINO   =>  " + a.ExistInDestinity.ToString()));
                scriptSql.WriteLine(CadenaFormateada("::"));
                if (!a.ExistInDestinity) {
                    scriptSql.WriteLine(CadenaFormateada("CREACION DE LA TABLA EN DESTINO"));
                    scriptSql.WriteLine(  a._queryCreateTable );
                    scriptSql.WriteLine(CadenaFormateada("::"));
                }
                if (a._queryDropTableTemp.Trim() != "") {
                    scriptSql.WriteLine(CadenaFormateada("ELIMINAMOS TABLA TEMPORAL EN DESTINO"));
                    scriptSql.WriteLine( a._queryDropTableTemp );
                    scriptSql.WriteLine(CadenaFormateada("::"));
                }
                if (a._queryCreateTableTemp.Trim() != "")
                {
                    scriptSql.WriteLine(CadenaFormateada("CREAMOS TABLA TEMPORAL EN DESTINO Y GUARDAMOS DATOS"));
                    scriptSql.WriteLine( a._queryCreateTableTemp );
                    scriptSql.WriteLine(CadenaFormateada("::"));
                }
                if (a._queryClearTable.Trim() != "")
                {
                    scriptSql.WriteLine(CadenaFormateada("LIMPIAMOS LA TABLA EN DESTINO"));
                    scriptSql.WriteLine( a._queryClearTable );
                    scriptSql.WriteLine(CadenaFormateada("::"));
                }
                scriptSql.WriteLine(CadenaFormateada("INSERTAMOS DATOS EN LA TABLA EN DESTINO"));
                if (a._queryInsertDatosDestino != null)
                {

                    scriptSql.WriteLine( a._queryLockTable );
                    scriptSql.WriteLine(CadenaFormateada("!40000 ALTER TABLE `"+a.NameDestino+"` DISABLE KEYS"));

                    foreach (string x in a._queryInsertDatosDestino)
                    { scriptSql.WriteLine(x); };
                    
                     
                    scriptSql.WriteLine(CadenaFormateada("!40000 ALTER TABLE `"+a.NameDestino + "` ENABLE KEYS"));
                    scriptSql.WriteLine("UNLOCK TABLES;");
                }
                else { scriptSql.WriteLine(CadenaFormateada("NO HAY DATOS PARA PASAR A TABLA EN DESTINO")); }
               

                if (a._queryDropTableTemp.Trim() != "")
                {
                    scriptSql.WriteLine(CadenaFormateada("ELIMINAMOS TABLA TEMPORAL EN DESTINO"));
                    scriptSql.WriteLine( a._queryDropTableTemp );
                    scriptSql.WriteLine(CadenaFormateada("::"));
                }

                scriptSql.WriteLine(CadenaFormateada("FIN MIGRACION TABLA  =>  " + a.Name));


            });
                scriptSql.Close();
            }
            catch (Exception ex) { Console.WriteLine("GenerarMigracion - "+ ex.ToString()); }
        }
        private void AplicarMigracion() {
            Console.WriteLine(CadenaFormateada("INICIO MIGRACION DE DATOS"));
            string toEnd = "";
            foreach (ValidacionTabla tblVal in this._tablasValidacion) {
                string str = "";
                if (toEnd.ToUpper() == "Y")
                {
                    break;
                }
                try {

                    
                    Console.WriteLine("\r"+CadenaFormateada("    TABLA   =>  " + tblVal.Name));
                    string lockTable, unlockTable;
                    str = tblVal._queryCreateTable; 
                    lockTable = tblVal._queryLockTable;
                    unlockTable = "UNLOCK TABLES;";
                    if (str.Trim() != "" ) {
                        lockTable = "";
                        unlockTable = "";
                    }
                    this._conn.ExecuteSQLQueries(str);
                    str = tblVal._queryDropTableTemp;
                    this._conn.ExecuteSQLQueries(str);
                    str = tblVal._queryCreateTableTemp;
                    this._conn.ExecuteSQLQueries(str);
                    str = tblVal._queryClearTable;
                    this._conn.ExecuteSQLQueries(str);
                    if (tblVal._queryInsertDatosDestino != null)
                    {
                       this._conn.ExecuteSQLQueries(lockTable);
                        foreach (string x in tblVal._queryInsertDatosDestino)
                        {
                            str = x;
                            this._conn.ExecuteSQLQueries(str);

                        }  
                       this._conn.ExecuteSQLQueries(unlockTable);
                    }
                    str = tblVal._queryDropTableTemp;
                    this._conn.ExecuteSQLQueries(str);
                } catch (Exception e)
                {
                    Console.WriteLine(CadenaFormateada("    ERROR EN CONSULTA"));
                    Console.WriteLine(CadenaFormateada(":"));
                    Console.WriteLine(CadenaFormateada("    CONSULTA"));
                    Console.WriteLine(str);
                    Console.WriteLine(CadenaFormateada(":"));
                    Console.WriteLine(CadenaFormateada("    ERROR"));
                    Console.WriteLine(e.Message);
                    Console.WriteLine(CadenaFormateada("    ERROR EN CONSULTA"));
                    toEnd = "";
                    while (toEnd.ToUpper() != "Y" && toEnd.ToUpper() != "N") {
                        Console.WriteLine(CadenaFormateada("¿Desea finalizar el proceso  (Y/N) ?"));
                        toEnd = Console.ReadLine();
                    }
                }


            }
            Console.WriteLine(CadenaFormateada("FIN MIGRACION DE DATOS"));
        }
        private void GenerarArchivoResultado() { 
            string fileName = "result_sinconizacion_" + DateTime.Now.GetHashCode().ToString();
            string fileHeader = "/*archivo backup de la base de datos actual \r\ngenerado automaticamente para recuperacion en caso de fallas\r\njdpsoluciones.com\r\njuniorjmd@gmail.com\r\n*/";
            scriptSql = new ScriptSql(fileName, config.GetNombreCarpetaMigracion(), "sql", fileHeader);

        }
        private ScriptSql GenerarArchivoAuxResultado()
        {
            string fileName = "result_sinconizacion_" + DateTime.Now.GetHashCode().ToString();
            string fileHeader = "/*archivo backup de la base de datos actual \r\ngenerado automaticamente para recuperacion en caso de fallas\r\njdpsoluciones.com\r\njuniorjmd@gmail.com\r\n*/";
            return new ScriptSql(fileName, config.GetNombreCarpetaMigracion(), "sql", fileHeader);

        }

        private string CadenaFormateada(string cadena) {
            string cadenaFinal;
            cadenaFinal = this.puntos.Remove(6) + cadena.Replace(" ", ":") + this.puntos.Substring(cadena.Length);
            return cadenaFinal;

        }
        private Boolean ValidarTablasDestino() {
            try {
                 tablasOrigen = _conn.CargartablasOrigenTabla();
                foreach (TablaPrincipal tabla   in  tablasOrigen )
                {
                    string tablaValidacion = tabla.Name;
                    ValidacionTabla validacion = new ValidacionTabla(tablaValidacion);
                    validacion.Name = config.GetNombreTabla(tablaValidacion);
                    validacion.ExistInDestinity = true;
                    //if (!_conn.ValidaTablaSqlServer(tabla.GetNameTableDestino()))
                    //{  
                    //    validacion.ExistInDestinity = false; 
                    ////  CrearConsultaCreateTableMysql(tablaValidacion, validacion);
                    //    CrearConsultaCreateTableSqlServer(tablaValidacion, validacion);
                    //}

                    var tipDestino = Config.GetTipoDestino();
                    switch (tipDestino)
                    {
                        case "MMSQL":
                            if (!_conn.ValidaTablaSqlServer(tabla.GetNameTableDestino()))
                            {
                                validacion.ExistInDestinity = false; 
                                CrearConsultaCreateTableSqlServer(tablaValidacion, validacion);
                            }
                            break;
                        case "MARIADB":
                        case "MYSQL":
                            if (!_conn.ValidaTablaMysql(tabla.GetNameTableDestino()))
                            {
                                validacion.ExistInDestinity = false;
                                CrearConsultaCreateTableMysql(tablaValidacion, validacion);
                            }
                            break;
                        default:
                            if (!_conn.ValidaTablaSqlServer(tabla.GetNameTableDestino()))
                            {
                                validacion.ExistInDestinity = false;
                                CrearConsultaCreateTableSqlServer(tablaValidacion, validacion);
                            }
                            break;
                    }
                    this._tablasValidacion.Add(validacion);
                }
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("ValidarTablasDestino : " + ex.ToString());
                return false;
            }
        }


        int  BuscarIndiceLista(string Name) {
            int i = 0 ;
            foreach (ValidacionTabla val in this._tablasValidacion) {

                if (val.Name == Name) {
                    break;
                }
                i++;

            } 
            return i;
        }

        private  TablaPrincipal  GetColumnasTablas(string NameTable) {
            TablaPrincipal lista = null;
            foreach (TablaPrincipal x in this.tablasOrigen)
            {
                if (x.Name == NameTable)
                {
                    lista = x;
                    break;
                }
            };
            return lista;
        }
        private void CrearConsultaCreateTableMysql(string NameTable , ValidacionTabla validacion )
        {

           TablaPrincipal dr_tabla_colum = GetColumnasTablas(NameTable); 


            string Query_ = "CREATE TABLE IF NOT EXISTS `" + dr_tabla_colum.GetNameTableDestino() + "` (";
            string coma = "";
            string ColPrimary = "";
            List<ColumDataBase> lista = dr_tabla_colum.columDataBase;
            lista.ForEach( col => 
            {
                string NotNull = "NOT";
                string TypeCol = "";



                string LengthCol ;
                string LengthColNumeric ;

                string ColAutoIncrement = "";
                LengthColNumeric = col.NUMERIC_PRECISION.ToString();
                if (LengthColNumeric == "") LengthColNumeric = "0";
                if (col.NUMERIC_SCALE.ToString() == "") col.NUMERIC_SCALE  = 2;
                LengthCol = col.CHARACTER_MAXIMUM_LENGTH.ToString();
                if (LengthCol == null) { LengthCol = "45"; }




                switch (col.DATA_TYPE.ToUpper())
                {   case "BIT":
                        TypeCol = "VARCHAR(" + LengthCol + ")"; 
                        break;
                    case "VARCHAR":
                    case "NVARCHAR":
                    case "CHAR":
                        TypeCol = "VARCHAR(" + LengthCol + ")"; 
                        break;
                    case "LONGTEXT":
                        TypeCol = "TEXT"; 
                        break;
                    case "INT":
                    case "BYTE":
                    case "GUID":
                    case "BIGBINARY":
                    case "LONGBINARY":
                    case "VARBINARY":
                    case "LONG":
                    case "SHORT":
                        TypeCol = "int(" + LengthColNumeric + ")";
                        break;
                    case "NUMERIC":
                    case "DECIMAL":
                    case "CURRENCY":
                        TypeCol = "decimal(" + LengthColNumeric + " , " + col.NUMERIC_SCALE.ToString() + ")";
                        break;
                    case "FLOAT": 
                        TypeCol = "FLOAT";
                        break;
                    case "SINGLE": 
                        TypeCol = "BIGINT";
                        break;
                    case "DOUBLE":
                        TypeCol = "DOUBLE";
                        break;
                    case "DATE":
                    case "DATETIME":
                         TypeCol = "DATETIME";
                      //  TypeCol = "VARCHAR(25)";
                        break;

                    case "BINARY": 
                    case "IMAGE":
                        TypeCol = "LONGBLOB";
                        break;
                }
                if (col.IS_NULLABLE) { NotNull = ""; }

                if (col.IS_PRIMARY)
                {
                    string auxcoma = coma;
                    if (ColPrimary == "") { auxcoma = ""; }
                    if (col.AUTO_INCREMENT)
                    {
                        if (auxcoma.Trim() == "") { 
                         ColPrimary = '`' + col.Name + '`';
                        }else {
                            ColPrimary =   '`' + col.Name + '`'+ auxcoma + ColPrimary;
                        }
                       
                    }
                    else {  ColPrimary += auxcoma + '`' + col.Name + '`'; }
                }
                if (col.AUTO_INCREMENT)
                {
                      ColAutoIncrement = "AUTO_INCREMENT";
                }
                Query_ += coma + "  `" + col.Name + "` " + TypeCol + " " + NotNull + " NULL " + ColAutoIncrement;
                coma = ",";
            }
             );
            //ColPrimary = "";
            if (ColPrimary != "")
            {
                Query_ += " , PRIMARY KEY (" + ColPrimary + ")";
            }
            Query_ += " ) ENGINE = InnoDB  DEFAULT CHARSET=latin1 ; ";

            validacion._queryCreateTable = Query_;  

        }

      
        private void CrearConsultaCreateTableSqlServer(string NameTable, ValidacionTabla validacion)
        {
            TablaPrincipal dr_tabla_colum = GetColumnasTablas(NameTable);
            var auxTabla = dr_tabla_colum.GetNameTableDestino().Replace("[", "").Replace("]", "");

            string Query_ = "IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '"+ auxTabla + 
                            "') BEGIN     CREATE TABLE    " + dr_tabla_colum.GetNameTableDestino() + "  (";
            string coma = "";
            string ColPrimary = "";
            List<ColumDataBase> lista = dr_tabla_colum.columDataBase;
          
            lista.ForEach(col =>
            {
                string NotNull = "NULL";
                string TypeCol = ""; 
                string ColIdentity = "";

                string LengthCol;
                string LengthColNumeric;
                //if (col.Name.ToUpper() == "RÉGIMEN" && auxTabla == "Beneficiarios") {
                //    Console.WriteLine("es regimen ooooo");
                //}
                LengthColNumeric = col.NUMERIC_PRECISION.ToString();
                if (LengthColNumeric == "") LengthColNumeric = "0";
                if (col.NUMERIC_SCALE.ToString() == "") col.NUMERIC_SCALE = 2;
                LengthCol = col.CHARACTER_MAXIMUM_LENGTH.ToString();
                if (LengthCol == null) { LengthCol = "0"; }
                if (LengthCol == "0") { LengthCol = "MAX"; } else {
                    LengthCol = (col.CHARACTER_MAXIMUM_LENGTH + 30 ).ToString();
                }

                if (auxTabla == "Memorandos")
                {
                    Console.WriteLine(col.DATA_TYPE.ToUpper() + " nombre : " + col.Name);
                }

                switch (col.DATA_TYPE.ToUpper())
                {
                    case "BIT":
                        TypeCol = "bit";
                        break;
                    case "VARCHAR":
                    case "NVARCHAR":
                    case "CHAR":
                        TypeCol = "VARCHAR(" + LengthCol + ")";
                        break;
                    case "LONGTEXT":
                        TypeCol = "TEXT";
                        break;
                    case "INT":
                    case "BIGBINARY": 
                        TypeCol = "[int]";
                        break; 
                    case "BYTE":
                        TypeCol = "[int]";
                        break;
                    case "LONG":
                        TypeCol = "[int]";
                        break;
                    case "SHORT": 
                        TypeCol = "[int]";
                        break;
                    case "CURRENCY":
                       TypeCol = "[money]";
                        break;
                    case "NUMERIC":
                    case "DECIMAL": 
                        // Ajustar el tipo de dato según la precisión y escala
                        TypeCol = $"[decimal]({col.NUMERIC_PRECISION}, {col.NUMERIC_SCALE})";
                        break;
                    case "FLOAT":
                        TypeCol = "FLOAT";
                        break;
                    case "SINGLE":
                    case "DOUBLE":
                        TypeCol = "FLOAT";
                        break;
                    case "DATE":
                    case "DATETIME":
                        TypeCol = "DATETIME";
                        break;
                    case "BINARY":
                    case "VARBINARY":
                    case "LONGBINARY":
                    case "GUID":
                        TypeCol = "VARBINARY(MAX)";
                        break;
                   
                }



                // Verificar si el nombre de la columna incluye la palabra "hora"
                if (col.Name.ToLower().Contains("hora"))
                {
                    TypeCol = "TIME";
                }
                if (col.Name.ToLower().Contains("fecha"))
                {
                    TypeCol = "DATE";
                }


                if (col.IS_NULLABLE) { NotNull = "NULL"; }

                if (col.IS_PRIMARY)
                {
                    ColPrimary += coma + "[" + col.Name + "]";
                }
                if (col.AUTO_INCREMENT)
                {
                    // En SQL Server, se utiliza IDENTITY para autoincrementar
                    ColIdentity = "IDENTITY(1,1)";
                }

                Query_ += coma + "  [" + col.Name + "] " + TypeCol + " " + NotNull + " " + ColIdentity;
                coma = ",";
            });

            if (ColPrimary != "")
            {
                Query_ += ", CONSTRAINT [PK_" + dr_tabla_colum.GetNameTableDestino() + "] PRIMARY KEY (" + ColPrimary + ")";
            }

            Query_ += "); END;";

            validacion._queryCreateTable = Query_;
        }

        public void CrearConsultaIngresarDatosMysql(TablaPrincipal TablaConsulta)
        {
            try
            {
                var list = new List<string>();
                var GroupDatosInsert = new List<string>();
                string[] columnas_;
                // TablaPrincipal dr_tabla_colum = GetColumnasTablas(TablaConsulta.Name);
                int IndexTablaListValidacion = BuscarIndiceLista(config.GetNombreTabla(TablaConsulta.Name));
                ValidacionTabla valTabla = this._tablasValidacion[IndexTablaListValidacion];

                int LimiteRegistrosPorConsulta = 150;
                string NombreTablaDestino = TablaConsulta.GetNameTableDestino();
                string HeaderQuery = "insert into `" + NombreTablaDestino + "` (";
                string MysqlQuery_ = "";
                string MysqlQuery0_ = "Drop table if EXISTS `temp_" + NombreTablaDestino + "`;";
                string MysqlQuery1_ = "Create table `temp_" + NombreTablaDestino +
                    "` select * from `" + NombreTablaDestino + "` ;";
                string MysqlQuery2_ = "Truncate table `" + NombreTablaDestino + "` ; ";
                string coma = "";
                int contadorDatos = 0;
                int datosEnDestino = 0;
                // OleDbDataReader dr_tabla_colum = this._conn.GetColumnasTabla(NameTable);



                OleDbDataReader dr_tabla = this._conn.DataReader(TablaConsulta.GetNameTableOrigen());

                if (valTabla.ExistInDestinity)
                {
                    SqlDataReader dr_TRegTablaDestino = this._conn.DataReaderAux(TablaConsulta.GetNameTableDestino(), new string[] { "count(0) as total" });
                    dr_TRegTablaDestino.Read();
                    datosEnDestino = int.Parse(dr_TRegTablaDestino["total"].ToString());
                    dr_TRegTablaDestino.Close();
                }


                OleDbDataReader dr_TRegTablaOrigen = this._conn.DataReader(TablaConsulta.GetNameTableOrigen(), new string[] { "count(0) as total" });
                dr_TRegTablaOrigen.Read();
                int datosEnOrigen = int.Parse(dr_TRegTablaOrigen["total"].ToString());
                dr_TRegTablaOrigen.Close();
                if (datosEnOrigen > 0)
                {
                    if (datosEnDestino > 0)
                    {
                        valTabla._queryDropTableTemp = MysqlQuery0_;
                        valTabla._queryCreateTableTemp = MysqlQuery1_;
                    }
                    valTabla.NameDestino = NombreTablaDestino;
                    valTabla._queryLockTable = "LOCK TABLES  `" + NombreTablaDestino + "` WRITE  ;";

                    TablaConsulta.columDataBase.ForEach(col =>
                    {
                        list.Add(col.Name);
                        HeaderQuery += coma + "  `" + col.Name + "`";
                        coma = ",";
                    });
                    columnas_ = list.ToArray();
                    HeaderQuery += " ) values ";
                    coma = "";
                    MysqlQuery_ = HeaderQuery;
                    while (dr_tabla.Read())
                    {
                        string comaInterno = "";
                        string auxCol = "";
                        MysqlQuery_ += coma + "(";


                        TablaConsulta.columDataBase.ForEach(col =>
                        {
                            string columna = col.Name;
                            auxCol = dr_tabla[columna].ToString();
                            auxCol = auxCol.Replace("\\", "\\\\");
                            auxCol = auxCol.Replace("'", "\\'");
                            auxCol = auxCol.Replace("\"", "\\" + '"');
                            switch (col.DATA_TYPE)
                            {
                                case "NUMERIC":
                                case "DECIMAL":
                                case "CURRENCY":
                                    auxCol = auxCol.Replace(",", ".");
                                    break;
                            }
                            if (dr_tabla[columna].ToString().Contains("a. m.")
                                || dr_tabla[columna].ToString().Contains("p. m.")
                                )
                            {  /*cambiar la fecha si el campo tiene p. m. o a. m. cambiar los datos 
                             por año-mes-dia horasMilitar:minutos:segundos*/
                                DateTime auxTime = DateTime.Parse(dr_tabla[columna].ToString());
                                auxCol = auxTime.ToString("yyyy-MM-dd HH:mm:ss");
                            }
                            MysqlQuery_ += comaInterno + "'" + auxCol + "'";
                            comaInterno = ",";
                        });
                        MysqlQuery_ += ")";
                        coma = ",";
                        contadorDatos++;
                        if (contadorDatos == LimiteRegistrosPorConsulta)
                        {
                            coma = "";
                            contadorDatos = 0;
                            MysqlQuery_ += " ;";
                            GroupDatosInsert.Add(MysqlQuery_);
                            MysqlQuery_ = HeaderQuery;
                        }
                    }
                    if (contadorDatos > 0 && contadorDatos <= LimiteRegistrosPorConsulta)
                    {
                        MysqlQuery_ += " ;";
                        GroupDatosInsert.Add(MysqlQuery_);
                    }
                    valTabla._queryClearTable = MysqlQuery2_;
                    valTabla._queryInsertDatosDestino = GroupDatosInsert;

                }
                _tablasValidacion[IndexTablaListValidacion] = valTabla;

            }
            catch (Exception e)
            { throw new Exception("CrearConsultaIngresarDatosMysql => " + e.ToString()); }

        }
        public void CrearConsultaIngresarDatosSqlServer(TablaPrincipal TablaConsulta)
        {
            try
            {
                var list = new List<string>();
                var GroupDatosInsert = new List<string>();
                string[] columnas_;
                // TablaPrincipal dr_tabla_colum = GetColumnasTablas(TablaConsulta.Name);
                int IndexTablaListValidacion = BuscarIndiceLista(config.GetNombreTabla(TablaConsulta.Name));
                ValidacionTabla valTabla = this._tablasValidacion[IndexTablaListValidacion];

                int LimiteRegistrosPorConsulta = 150;
                string NombreTablaDestino = TablaConsulta.GetNameTableDestino();
                string HeaderQuery = "INSERT INTO  " + NombreTablaDestino + "  (";
                string MysqlQuery_ = "";
                string MysqlQuery0_ = "DROP TABLE IF EXISTS  " + TablaConsulta.GetNameTableTempDestino()  ;
                string MysqlQuery1_ = "SELECT * INTO  "   +TablaConsulta.GetNameTableTempDestino() + " FROM  " + NombreTablaDestino  ;
                string MysqlQuery2_ = "TRUNCATE TABLE " + NombreTablaDestino ;
                string coma = "";
                int contadorDatos = 0;
                int datosEnDestino = 0;
                // OleDbDataReader dr_tabla_colum = this._conn.GetColumnasTabla(NameTable);



                OleDbDataReader dr_tabla = this._conn.DataReader(TablaConsulta.GetNameTableOrigen());

                if (valTabla.ExistInDestinity)
                {
                    SqlDataReader dr_TRegTablaDestino = this._conn.DataReaderAux(TablaConsulta.GetNameTableDestino(), new string[] { "count(0) as total" });
                    dr_TRegTablaDestino.Read();
                    datosEnDestino = int.Parse(dr_TRegTablaDestino["total"].ToString());
                    dr_TRegTablaDestino.Close();
                }


                OleDbDataReader dr_TRegTablaOrigen = this._conn.DataReader(TablaConsulta.GetNameTableOrigen(), new string[] { "count(0) as total" });
                dr_TRegTablaOrigen.Read();
                int datosEnOrigen = int.Parse(dr_TRegTablaOrigen["total"].ToString());
                dr_TRegTablaOrigen.Close();
                if (datosEnOrigen > 0)
                {
                    if (datosEnDestino > 0)
                    {
                        valTabla._queryDropTableTemp = MysqlQuery0_;
                        valTabla._queryCreateTableTemp = MysqlQuery1_;
                    }
                    valTabla.NameDestino = NombreTablaDestino;
                    valTabla._queryLockTable = "ALTER TABLE  " + NombreTablaDestino + "  DISABLE TRIGGER ALL";

                    TablaConsulta.columDataBase.ForEach(col =>
                    {
                        list.Add(col.Name);
                        HeaderQuery += coma + "  [" + col.Name + "]";
                        coma = ",";
                    });
                    columnas_ = list.ToArray();
                    HeaderQuery += " ) values ";
                    coma = "";
                    MysqlQuery_ = HeaderQuery;
                    while (dr_tabla.Read())
                    {
                        string comaInterno = "";
                        string auxCol = "";
                        MysqlQuery_ += coma + "(";


                        TablaConsulta.columDataBase.ForEach(col =>
                        {
                            string columna = col.Name;

                           //if ( NombreTablaDestino == "[Clientes]" ) {
                               
                           //     Console.WriteLine(col.Name + " cliente");
                           //     if (col.Name == "InterésFijoMensual") {

                           //         Console.WriteLine(col.Name + " ese es");
                           //     }
                           //}

                            auxCol = dr_tabla[columna].ToString().Trim();
                            auxCol = auxCol.Replace("\\", "\\\\");
                            auxCol = auxCol.Replace("'", "''");
                            auxCol = auxCol.Replace("\"", "\\" + '"');
                            switch (col.DATA_TYPE.ToUpper())
                            {
                                case "NUMERIC":
                                case "DECIMAL":
                                case "CURRENCY":
                                case "FLOAT": 
                                case "INT":
                                case "DOUBLE":
                                case "SINGLE":
                                    auxCol = auxCol.Replace(",", ".");
                                    break;
                                case "BINARY":
                                case "VARBINARY":
                                case "LONGBINARY":
                                    if (auxCol.Trim() == "") auxCol = "NULL";
                                    break;
                                case "BIT":
                                    if (auxCol.ToUpper() == "FALSE")
                                    {
                                        auxCol = "0";
                                    }
                                    else { auxCol = "1"; }
                                break;
                                
                            }
                            if (dr_tabla[columna].ToString().Contains("a. m.")
                                || dr_tabla[columna].ToString().Contains("p. m.")
                                )
                            {  /*cambiar la fecha si el campo tiene p. m. o a. m. cambiar los datos 
                             por año-mes-dia horasMilitar:minutos:segundos*/
                                DateTime auxTime = DateTime.Parse(dr_tabla[columna].ToString());
                                auxCol = auxTime.ToString("yyyy-MM-dd HH:mm:ss");
                            }
                        if (col.Name.ToLower().Contains("hora") || col.Name.ToLower().Contains("fecha"))
                            {
                                if (auxCol.Trim() == "0") auxCol = "NULL";
                            }
                   


                            switch (col.DATA_TYPE.ToUpper())
                            {
                                case "NUMERIC":
                                case "DECIMAL":
                                case "CURRENCY":
                                case "FLOAT":
                                case "BIT":
                                case "INT":
                                case "DOUBLE":
                                case "SINGLE":
                                    if (auxCol.Trim() == "") auxCol = "0"; 
                                    MysqlQuery_ += comaInterno +  auxCol  ;
                                break;
                                
                                default:
                                    if (auxCol.Trim() == "NULL")
                                    {
                                        MysqlQuery_ += comaInterno  + auxCol  ;
                                    }
                                    else { MysqlQuery_ += comaInterno + "'" + auxCol + "'"; }
                                    
                                    break;
                            }


                            comaInterno = ",";
                        });
                        MysqlQuery_ += ")";
                        coma = ",";
                        contadorDatos++;
                        if (contadorDatos == LimiteRegistrosPorConsulta)
                        {
                            coma = "";
                            contadorDatos = 0;
                            MysqlQuery_ += " ;";
                            GroupDatosInsert.Add(MysqlQuery_);
                            MysqlQuery_ = HeaderQuery;
                        }
                    }
                    if (contadorDatos > 0 && contadorDatos <= LimiteRegistrosPorConsulta)
                    {
                        MysqlQuery_ += " ;";
                        GroupDatosInsert.Add(MysqlQuery_);
                    }
                    valTabla._queryClearTable = MysqlQuery2_;
                    valTabla._queryInsertDatosDestino = GroupDatosInsert;

                }
                _tablasValidacion[IndexTablaListValidacion] = valTabla;

            }
            catch (Exception e)
            { throw new Exception("CrearConsultaIngresarDatosSQL => " + e.ToString()); }

        }
        

        private Boolean PasarDatosATablasDestino()
        {
            try
            { 
                this.tablasOrigen.ForEach(a => {



                    var tipDestino = Config.GetTipoDestino();
                    switch (tipDestino)
                    {
                        case "MMSQL":
                            CrearConsultaIngresarDatosSqlServer(a);
                            break;
                        case "MARIADB":
                        case "MYSQL":
                            CrearConsultaIngresarDatosMysql(a);
                            break;
                        default:
                            CrearConsultaIngresarDatosSqlServer(a);
                            break;
                    } 
                    Console.WriteLine("termino query ingresar datos tabla  : " + a.Name);
                }); 
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("PasarDatosATablasDestino : " + ex.ToString());
                return false;
            }
        }

    }
}
