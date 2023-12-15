using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace siigo 
{
    public class ColumDataBase
    {
        public string Name { get; set; }
        public string DATA_TYPE { get; set; }
        public int CHARACTER_MAXIMUM_LENGTH { get; set; } 
        public int NUMERIC_PRECISION { get; set; }
        public int NUMERIC_SCALE { get; set; }
        public bool IS_NULLABLE { get; set; }
        public bool AUTO_INCREMENT { get; set; }
        public bool IS_PRIMARY { get; set; }

        public ColumDataBase()
        {
            Name = "";
            DATA_TYPE = "INT";
            CHARACTER_MAXIMUM_LENGTH = 0;
            NUMERIC_PRECISION = 0;
            NUMERIC_SCALE = 0;
            IS_NULLABLE = true;
            AUTO_INCREMENT = false;
            IS_PRIMARY = false;
        }

    }
}
