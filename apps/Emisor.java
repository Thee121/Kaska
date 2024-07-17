// Ejemplo de programa que usa Kaska.
// Crea temas y envía mensajes de distintos tipos

package apps;
import java.rmi.RemoteException;
import java.rmi.NotBoundException;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import kaska.KaskaClient;
import kaska.Record;

class Emisor {
    static public void main(String args[]) {
        args = new String[]{"localhost","1099"};
        if (args.length!=2) {
            System.err.println("Usage: Test registryHost RegistryPort");
            return;
        }
        try {
            KaskaClient cl;
            // se conecta con el broker
            cl = new KaskaClient(args[0], args[1]);

            // crea 2 temas
            int v = cl.createTopics(Arrays.asList("Puntos", "Palabras"));

            // envío de un objeto de una clase estándar
            cl.send("Palabras", "hola");

            // envío de una colección que contiene objetos de una clase estándar
            List<String> lpa = Arrays.asList("hasta", "luego");
            cl.send("Palabras", lpa);

            // envío de un objeto de una clase definida por el usuario
            Point p = new Point(111, 222);
            cl.send("Puntos", p);

            // envío de colección con objetos de una clase definida por el usuario
            List<Point> lpo=Arrays.asList(new Point(333, 444), new Point());
            cl.send("Puntos", lpo);

        } catch (NotBoundException e) {
            System.err.println("error localizando registry "+ e.toString());
            return;
        } catch (RemoteException e) {
            System.err.println("error de comunicación "+ e.toString());
            return;
        }
    }
}
