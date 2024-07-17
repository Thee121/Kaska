// Cliente para probar el servicio.
package apps;

import java.util.Scanner;
import java.util.ArrayList;
import java.rmi.RemoteException;
import java.rmi.NotBoundException;
import kaska.KaskaClient;
import kaska.Record;
import java.lang.reflect.InvocationTargetException;

class Test {
    static char car = 'A';

    static private boolean doCreateTopics(KaskaClient cl, Scanner ent) throws RemoteException {
        System.err.println("Introduzca en la misma línea el nombre de los temas que quiere crear: ");
        if (!ent.hasNextLine())
            return false;
        String lin = ent.nextLine();
        ArrayList<String> l = new ArrayList<>();
        Scanner s = new Scanner(lin);
        while (s.hasNext())
            l.add(s.next());
        int n = cl.createTopics(l);
        System.err.println("createTopic ha devuelto un valor " + n);
        return true;
    }

    static private boolean doTopicList(KaskaClient cl, Scanner ent) throws RemoteException {
        System.err.println("topicList ha devuelto: " + cl.topicList());
        return true;
    }

    static private boolean doSendString(KaskaClient cl, Scanner ent) throws RemoteException {
        System.err.println("Introduzca el nombre del tema: ");
        if (!ent.hasNextLine())
            return false;
        String lin = ent.nextLine();
        Scanner s = new Scanner(lin);
        if (!s.hasNext())
            return false;
        String topic = s.next();
        int tam = 32;
        byte[] buf = new byte[tam];
        for (int i = 0; i < tam; i++)
            buf[i] = (byte) car;
        ++car;
        String mess = new String(buf);
        System.err.println("Se va a escribir el string \"" + mess + "\" en el tema " + topic);
        System.err.println("send ha devuelto " + cl.send(topic, mess));
        return true;
    }

    static private boolean doGet(KaskaClient cl, Scanner ent) throws RemoteException {
        System.err.println("Introduzca en la misma línea el nombre del tema y el offset: ");
        if (!ent.hasNextLine())
            return false;
        String lin = ent.nextLine();
        Scanner s = new Scanner(lin);
        if (!s.hasNext())
            return false;
        String topic = s.next();
        if (!s.hasNextInt())
            return false;
        int off = s.nextInt();
        Record r = cl.get(topic, off);
        Object m;
        if (r == null)
            System.err.println("get ha devuelto un error");
        else
            System.err.println("Mensaje recibido: tema " + r.getTopic() + " offset " + r.getOffset() + " contenido "
                    + (m = r.getMessage()) + " " + m.getClass());

        return true;
    }

    static private boolean doSubscribe(KaskaClient cl, Scanner ent) throws RemoteException {
        System.err.println("Introduzca en la misma línea el nombre de los temas a suscribirse: ");
        if (!ent.hasNextLine())
            return false;
        String lin = ent.nextLine();
        ArrayList<String> l = new ArrayList<>();
        Scanner s = new Scanner(lin);
        while (s.hasNext())
            l.add(s.next());
        int n = cl.subscribe(l);
        System.err.println("subscribe ha devuelto un valor " + n);
        return true;
    }

    static private boolean doUnsubscribe(KaskaClient cl, Scanner ent) {
        cl.unsubscribe();
        System.err.println("unsubscribe realizado");
        return true;
    }

    static private boolean doPosition(KaskaClient cl, Scanner ent) {
        System.err.println("Introduzca el nombre del tema: ");
        if (!ent.hasNextLine())
            return false;
        String lin = ent.nextLine();
        Scanner s = new Scanner(lin);
        if (!s.hasNext())
            return false;
        String topic = s.next();
        int off = cl.position(topic);
        if (off == -1)
            System.err.println("position ha devuelto un error");
        else
            System.err.println("offset recibido " + off);
        return true;
    }

    static private boolean doSeek(KaskaClient cl, Scanner ent) {
        System.err.println("Introduzca en la misma línea el nombre del tema y el offset: ");
        if (!ent.hasNextLine())
            return false;
        String lin = ent.nextLine();
        Scanner s = new Scanner(lin);
        if (!s.hasNext())
            return false;
        String topic = s.next();
        if (!s.hasNextInt())
            return false;
        int off = s.nextInt();
        System.err.println("seek ha devuelto un valor " + cl.seek(topic, off));
        return true;
    }

    static private boolean doPoll(KaskaClient cl, Scanner ent) throws RemoteException {
        System.err.println("Mensajes recibidos (en caso de que haya)");
        Object m;
        for (Record r : cl.poll())
            System.err.println("\ttema " + r.getTopic() + " offset " + r.getOffset() + " contenido "
                    + (m = r.getMessage()) + " " + m.getClass());
        return true;
    }

    static private void prompt() {
        System.err.println("Introduzca operacion (Ctrl-D para terminar)");
        System.err.println(
                "\toperaciones: createTopics|topicList|sendString|SendClass|get|subscribe|unsubscribe|position|seek|poll");
    }

    static private boolean doSendClass(KaskaClient cl, Scanner ent) throws RemoteException {
        System.err.println(
                "Introduzca el nombre del tema y el nombre completo de la clase (paquete.clase) de la que quiere enviar un objeto: ");
        if (!ent.hasNextLine())
            return false;
        String lin = ent.nextLine();
        Scanner s = new Scanner(lin);
        if (!s.hasNext())
            return false;
        String topic = s.next();
        if (!s.hasNext())
            return false;
        String clase = s.next();
        Object o;
        try {
            o = Class.forName(clase).getConstructor().newInstance();
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException
                | InvocationTargetException e) {
            System.err.println("error en clase especificada");
            return false;
        }
        System.err.println("Se va a escribir el objeto " + o + " de la clase " + clase + " en el tema " + topic);
        System.err.println("send ha devuelto " + cl.send(topic, o));
        return true;
    }

    static public void main(String args[]) {
        args = new String[] { "localhost", "1099" };

        if (args.length != 2) {
            System.err.println("Usage: Test registryHost RegistryPort");
            return;
        }
        try {
            KaskaClient cl;
            cl = new KaskaClient(args[0], args[1]);
            if (cl == null)
                return;
            while (true) {
                boolean formatoOK = false;
                Scanner ent = new Scanner(System.in);
                prompt();
                if (!ent.hasNextLine())
                    return;
                String lin = ent.nextLine();
                Scanner s = new Scanner(lin);
                if (s.hasNext()) {
                    String op = s.next();
                    switch (op) {
                        case "createTopics":
                            formatoOK = doCreateTopics(cl, ent);
                            break;
                        case "topicList":
                            formatoOK = doTopicList(cl, ent);
                            break;
                        case "sendString":
                            formatoOK = doSendString(cl, ent);
                            break;
                        case "sendClass":
                            formatoOK = doSendClass(cl, ent);
                            break;
                        case "get":
                            formatoOK = doGet(cl, ent);
                            break;
                        case "subscribe":
                            formatoOK = doSubscribe(cl, ent);
                            break;
                        case "unsubscribe":
                            formatoOK = doUnsubscribe(cl, ent);
                            break;
                        case "position":
                            formatoOK = doPosition(cl, ent);
                            break;
                        case "seek":
                            formatoOK = doSeek(cl, ent);
                            break;
                        case "poll":
                            formatoOK = doPoll(cl, ent);
                            break;
                    }
                }
                if (!formatoOK)
                    System.err.println("Error en formato de operacion");
            }
        } catch (NotBoundException e) {
            System.err.println("error localizando registry " + e.toString());
            return;
        } catch (RemoteException e) {
            System.err.println("error de comunicación " + e.toString());
            return;
        } catch (Exception e) {
            System.err.println("excepción en la ejecución del Test: " + e.toString());
        } finally {
            System.exit(0);
        }

    }
}
