using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace siigo
{
    public class Program
    {
        static void Main(string[] args)
        {
            if (args.Any())
            {
                System.Console.WriteLine("Bienvenid@ " + args[0]);
                int function_ = int.Parse(args[0]);
            switch (function_) {
                case 1:
                    try
                    {
                        Factura factura = new Factura(args[1], int.Parse(args[2]));
                        factura.Listar();
                    }catch(Exception ex) { 
                        Console.WriteLine(ex.ToString());
                    
                    }
                    break; 
                case 2:
                    try
                    {
                        Sincronizacion sinc = new Sincronizacion();
                            sinc.Start();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());

                    }
                    break;
                default:
                    Console.WriteLine("Servicio activo");
                break;

            }
            }
            else
            {
                Console.WriteLine("Sin parámetros");
            }

           // string toEnd = Console.ReadLine();


             


        }
    }
}
