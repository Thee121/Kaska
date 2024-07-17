// Clase de cliente que proporciona los métodos para interacciona con el broker.
// Corresponde al API ofrecida a las aplicaciones 
package kaska;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.NotBoundException;
import java.util.Collection;
import java.util.List;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import kaskaprot.KaskaSrv;
import kaskaprot.TopicWithOffset;

public class KaskaClient {
    private KaskaSrv servidorKaska;
    private Map<String, TopicWithOffset> subscripcionesMapa;

    // constructor: realiza el lookup del servicio en el Registry
    public KaskaClient(String host, String port) throws RemoteException, NotBoundException {
        Registry registro = LocateRegistry.getRegistry(host, Integer.parseInt(port));
        servidorKaska = (KaskaSrv) registro.lookup("Kaska");
    }

    // se crean temas devolviendo cuántos se han creado
    public int createTopics(Collection<String> topics) throws RemoteException {
        // primero elimino duplicados
        HashSet<String> topicSet = new HashSet<>(topics);
        int valor = servidorKaska.createTopics(topicSet);
        return valor;
    }

    // función de conveniencia para crear un solo tema
    public boolean createOneTopic(String topic) throws RemoteException {
        return createTopics(Arrays.asList(topic)) == 1;
    }

    // devuelve qué temas existen
    public Collection<String> topicList() throws RemoteException {
        Collection<String> listaTopics = servidorKaska.topicList();
        return listaTopics;
    }

    // envía un mensaje devolviendo error si el tema no existe o el
    // objeto no es serializable
    public boolean send(String topic, Object o) throws RemoteException {
        byte[] mensaje = serializeMessage(o);
        if (mensaje == null) {
            return false;
        }
        boolean enviado = servidorKaska.send(topic, mensaje);
        return enviado;
    }

    // lee mensaje pedido de un tema devolviéndolo en un Record
    // (null si error: tema no existe o mensaje no existe);
    // se trata de una función para probar el buen funcionamiento de send;
    // en Kafka los mensajes se leen con poll
    public Record get(String topic, int offset) throws RemoteException {
        byte[] mensaje = servidorKaska.get(topic, offset);
        if(mensaje == null){
            return null;
        }
        Record recordMensaje = new Record(topic,offset,mensaje);
        return recordMensaje;
    }

    // se suscribe a los temas pedidos devolviendo a cuántos se ha suscrito
    public int subscribe(Collection<String> topics) throws RemoteException {
        // primero elimino duplicados
        HashSet<String> topicSet = new HashSet<>(topics);
        Collection<TopicWithOffset> offsets = servidorKaska.endOffsets(topicSet);
        subscripcionesMapa = offsets.stream().collect(Collectors.toMap(TopicWithOffset::getTopic, Function.identity()));
        int tamano = subscripcionesMapa.size();
        return tamano;
    }

    // función de conveniencia para suscribirse a un solo tema
    public boolean subscribeOneTopic(String topic) throws RemoteException {
        return subscribe(Arrays.asList(topic)) == 1;
    }

    // se da de baja de todas las suscripciones
    public void unsubscribe() {
        subscripcionesMapa.clear();
    }

    // obtiene el offset local de un tema devolviendo -1 si no está suscrito a ese
    // tema
    public int position(String topic) {
        TopicWithOffset topicOffset = subscripcionesMapa.get(topic);
		if (topicOffset == null) {
			return -1;
		}
        int offsetLocal = topicOffset.getOffset();
		return offsetLocal;
    }

    // modifica el offset local de un tema devolviendo error si no está suscrito a
    // ese tema
    public boolean seek(String topic, int offset) {
        boolean respuesta = false;
        TopicWithOffset topicOffset = subscripcionesMapa.get(topic);
        if(topicOffset == null){
            respuesta = false;
        }else{
            respuesta = true;
            topicOffset.setOffset(offset);
        }
        return respuesta;
    }

    // obtiene todos los mensajes no leídos de los temas suscritos
    // devuelve null si no está suscrito a ningún tema
    public List<Record> poll() throws RemoteException {
        return null;
    }

    // función interna que serializa un objeto en un array de bytes
    // devuelve null si el objeto no es serializable
    byte[] serializeMessage(Object o) {
        byte[] b = null;
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(o);
            b = bos.toByteArray();
        } catch (IOException e) {
            System.err.println("error en la serialización " + e.toString());
        }
        return b;
    }
}
