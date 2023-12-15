using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using static Org.BouncyCastle.Math.EC.ECCurve;

namespace siigo 
{
    public class TablaPrincipal
    {
        public string Name = "";
        public List<string> primaryKey = new List<string>();  
        public List<ColumDataBase> columDataBase = new List<ColumDataBase>();
        public TablaPrincipal(string NameTabla, List<string> primaria)
        {
            this.Name = NameTabla; 
            this.primaryKey = primaria;

        }
        public string GetNameTableOrigen() {
            return "[" + Name + "]";
        }
        public string GetNameTableDestino() {
            string retorno = Name;
            // retorno = retorno.Replace('á', 'a');
            // retorno = retorno.Replace('é', 'e');
            // retorno = retorno.Replace('í', 'i');
            // retorno = retorno.Replace('ó', 'o');
            // retorno = retorno.Replace('ú', 'u');
            //retorno = retorno.Replace(' ', '_');
            Config config = Config.GetInstance();
             return config.GetNombreTabla(retorno); 
        }

        public string GetNameTableTempDestino()
        {
            string retorno = Name;
            // retorno = retorno.Replace('á', 'a');
            // retorno = retorno.Replace('é', 'e');
            // retorno = retorno.Replace('í', 'i');
            // retorno = retorno.Replace('ó', 'o');
            // retorno = retorno.Replace('ú', 'u');
            //retorno = retorno.Replace(' ', '_');
            Config config = Config.GetInstance();
            return config.GetNombreTabla("tmp_"+retorno);
        }

    }
}
