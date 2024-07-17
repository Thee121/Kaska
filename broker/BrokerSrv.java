// Servidor que implementa la interfaz remota Kaska
package broker;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import kaskaprot.KaskaSrv;
import kaskaprot.TopicWithOffset;

class BrokerSrv extends UnicastRemoteObject implements KaskaSrv {
    public static final long serialVersionUID = 1234567890L;

    private Map<String, List<byte[]>> temas;

    public BrokerSrv() throws RemoteException {
        temas = new ConcurrentHashMap<>();
    }

    // se crean temas devolviendo cuántos se han creado
    public synchronized int createTopics(Collection<String> topics) throws RemoteException {
        int contadorTopics = 0;
        Iterator<String> itTemas = topics.iterator();

        while (itTemas.hasNext()) {
            String temaActual = itTemas.next().toString();
            if (!this.temas.containsKey(temaActual)) {
                temas.put(temaActual, new ArrayList<>());
                contadorTopics++;
            }
        }
        return contadorTopics;
    }

    // devuelve qué temas existen
    public synchronized Collection<String> topicList() throws RemoteException {
        return new ArrayList<>(temas.keySet());
    }

    // envía un array bytes devolviendo error si el tema no existe
    public synchronized boolean send(String topic, byte[] m) throws RemoteException {
        List<byte[]> mensajesTopic = temas.get(topic);

        if (mensajesTopic == null) {
            return false;
        }
        mensajesTopic.add(m);
        return true;
    }

    // lee un determinado mensaje de un tema devolviendo null si error
    // (tema no existe o mensaje no existe)
    // se trata de una función para probar el buen funcionamiento de send;
    // en Kafka los mensajes se leen con poll
    public synchronized byte[] get(String topic, int offset) throws RemoteException {
        List<byte[]> mensajesTopic = temas.get(topic);
        if (mensajesTopic == null || offset >= mensajesTopic.size()) {
            return null;
        }
        byte[] respuesta = mensajesTopic.get(offset);
        return respuesta;
    }

    // obtiene el offset actual de estos temas en el broker ignorando
    // los temas que no existen
    public synchronized Collection<TopicWithOffset> endOffsets(Collection<String> topics) throws RemoteException {
        List<TopicWithOffset> offsets = new ArrayList<>();
        Iterator<String> temasIterator = topics.iterator();

        while (temasIterator.hasNext()) {

            String temaActual = temasIterator.next().toString();
            List<byte[]> mensajesTopic = this.temas.get(temaActual);

            if (mensajesTopic != null) {
                TopicWithOffset topicsOffset = new TopicWithOffset(temaActual, mensajesTopic.size());
                offsets.add(topicsOffset);
            }
        }
        return offsets;
    }

    // obtiene todos los mensajes no leídos de los temas suscritos
    public synchronized Map<String, List<byte[]>> poll(Collection<TopicWithOffset> topics) throws RemoteException {
        return null;
    }

    static public void main(String args[]) {
        if (args.length != 1) {
            System.err.println("Usage: BrokerSrv registryPortNumber");
            return;
        }
        try {
            BrokerSrv srv = new BrokerSrv();
            Registry registry = LocateRegistry.getRegistry(Integer.parseInt(args[0]));
            registry.rebind("Kaska", srv);
        } catch (Exception e) {
            System.err.println("Broker exception: " + e.toString());
            System.exit(1);
        }
    }
}
