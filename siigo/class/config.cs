using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace siigo 
{
    public class Config
    {
        private string UserDataBase;
        private string PassDataBase;
        private string NameDataBase;
        private string Url;
        private string UrlPad;
        private string PrefijoTabla;

        private string AxUserDataBase;
        private string AxPassDataBase;
        private string AxNameDataBase;
        private string PathAccessDataBase;
        private string NameAccessDataBase;
        private string ExtAccessDataBase;
        private string DSAccessDataBase;
        private static string TipoDestino;
        private static Config _instance;


        /*DESKTOP-SFFU202\MSSQLSERVER01
         * <add name="cn" connectionString="Data Source=DESKTOP-SFFU202\MSSQLSERVER01;
         * Initial Catalog=Africanas;Persist Security Info=True;User ID=sa;Password=123456789 ; TRUSTED_CONNECTION = TRUE" providerName="System.Data.SqlClient" />
*/

        private Config()
        {
            Url = "";
            UserDataBase = "sa";
            PassDataBase = "123456789";
         //   NameDataBase = "migracionCT";
            NameDataBase = "controlTotalDiciembre";
            AxUserDataBase = "jdpsoluc_africanas_user";
            AxPassDataBase = "Qazwsxedc345*";
            AxNameDataBase = "jdpsoluc_africanas_1";
            TipoDestino = "MMSQL";
          //  UrlPad = @"D:\jdpSincAccessDatabase\"; 
            UrlPad = @"D:\sincNuevoJdsCT\"; 
            PrefijoTabla = "";
            PathAccessDataBase = @"C:\Users\junio\source\repos\CT-jds.net\assets\ContolTotalAccessDB\";
            // PathAccessDataBase = @"D:\creacion remisiones continental\PRINCIPAL\";
            //NameAccessDataBase = "ControlDBP1";
            NameAccessDataBase = "ControlDBP1";
            ExtAccessDataBase = "mdb";
            //ExtAccessDataBase = "accdb";
            DSAccessDataBase = PathAccessDataBase+ NameAccessDataBase+'.'+ ExtAccessDataBase; 

        }
        public static Config GetInstance()
        {
            if (_instance == null)
            {
                _instance = new Config();
            }
            return _instance;
        }

        public static string GetTipoDestino()
        { 
            return TipoDestino;
        }
        public string GetNombreTabla(string Ntabla)
        {
            
            switch (TipoDestino)
            {
                case "MMSQL":
                case "ACCESS":
                    return '['+ PrefijoTabla + Ntabla + ']';
                case "MARIADB":
                case "MYSQL":
                    return '`' + PrefijoTabla + Ntabla + '`';
                default: return PrefijoTabla + Ntabla ;

            }
        }


        public string GetNombreAccessDB()
        {
            return NameAccessDataBase;
        }

        public string GetConnetionString() {
            return "Data Source=DESKTOP-SFFU202\\MSSQLSERVER01;Initial Catalog="
                          + this.NameDataBase + ";Persist Security Info=True;" +
                          "User ID=" + this.UserDataBase + ";Password=" + this.PassDataBase + " ; MultipleActiveResultSets=True ;" +
                          " TRUSTED_CONNECTION = TRUE ";
     
        }
        public string GetAccessConnetionString()
        {
            return "Provider=Microsoft.ACE.OLEDB.12.0; Data Source = " + DSAccessDataBase;

        } 
        public string GetConnetionStringAux()
        {  
            return "Server=jdpsoluciones.com;Database="+this.AxNameDataBase+"; Uid="+this.AxUserDataBase+";Pwd="+this.AxPassDataBase+";";

        }

        public string GetNombreCarpetaMigracion()
        {
            return UrlPad;

        }

    }
}
