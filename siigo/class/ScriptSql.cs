using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace siigo 
{
    public  class ScriptSql
    {
        public string name;
        public string path;
        public string extention;

        private string header;
        private string folder;
        private StreamWriter file;
        public ScriptSql(string name, string path, string extention, string header , string folder = null) {
            this.name = name;
            this.path = path;
            this.extention = extention;
            this.header = header;
            this.folder = folder;
            CreateFileResult();
    }
        public void WriteLine(string line  ) {
            file.WriteLine(line);
            string print = line;
            if(print.Length > 150) {
                print = print.Substring(0, 150) + "...";
            }
            Console.WriteLine(print);
            
        }

        public void Close()
        {
            this.file.Close();
        }
        public void CrearNuevoArchivo(string name, string path, string extention, string header, string folder = null)
        {
            this.name = name;
            this.path = path;
            this.extention = extention;
            this.header = header;
            this.folder = folder;
            this.Close();
            CreateFileResult();
        }
        private void CreateFileResult()
        {
            Console.WriteLine(path + name.Trim() + "." + extention);
            try
            {
                string auxPath = path;
                if (folder != null) {
                    auxPath += folder+'\\';
                }

                bool exists = System.IO.Directory.Exists(auxPath);
                if (!exists) System.IO.Directory.CreateDirectory(auxPath);
                file = new StreamWriter(auxPath + name + "." + extention);
                file.WriteLine(header);
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }

        }



    }
}
