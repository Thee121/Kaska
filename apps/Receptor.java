// Ejemplo de programa que usa Kaska.
// Se suscribe a temas existentes y recibe mensajes de distintos tipos
// Debería ejecutarse antes el Emisor

package apps;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.rmi.RemoteException;
import java.rmi.NotBoundException;
import kaska.KaskaClient;
import kaska.Record;

class Receptor {
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

            // se suscribe a temas ya existentes
            int v = cl.subscribe(Arrays.asList("Puntos", "Palabras"));
            if (v == 0) {
                System.err.println("no se ha suscrito a ningún tema");
                return;
            }

            System.err.println("Mensajes recibidos (no habrá porque Emisor los envió antes de la suscripción)");
            // lee todos los mensajes pendientes
            Object m;
            for (Record r: cl.poll())
                System.err.println("\ttema \"" + r.getTopic() + "\" offset " + r.getOffset() + " contenido \"" + (m = r.getMessage()) + "\" " + m.getClass());

            // fija el offset a 0 para ambos temas
            cl.seek("Palabras", 0);
            cl.seek("Puntos", 0);

            System.err.println("Mensajes recibidos (sí habrá porque ha movido el offset a 0)");
            // vuelve a leer todos los mensajes pendientes
            for (Record r: cl.poll())
                System.err.println("\ttema \"" + r.getTopic() + "\" offset " + r.getOffset() + " contenido \"" + (m = r.getMessage()) + "\" " + m.getClass());

        } catch (NotBoundException e) {
            System.err.println("error localizando registry "+ e.toString());
            return;
        } catch (RemoteException e) {
            System.err.println("error de comunicación "+ e.toString());
            return;
        }
    }
}
