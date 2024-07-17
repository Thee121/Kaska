// Interfaz remota Kaska
// NO MODIFICAR

package kaskaprot;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface KaskaSrv extends Remote {

    // se crean temas devolviendo cuántos se han creado
    int createTopics(Collection <String> topics) throws RemoteException;
    
    // devuelve qué temas existen
    Collection <String> topicList() throws RemoteException;
    
    // envía un array bytes devolviendo error si el tema no existe
    boolean send(String topic, byte[] m) throws RemoteException;

    // lee un determinado mensaje de un tema devolviendo null si error
    // (tema no existe o mensaje no existe)
    // se trata de una función para probar el buen funcionamiento de send;
    // en Kafka los mensajes se leen con poll
    byte [] get(String topic, int offset) throws RemoteException;

    // obtiene el offset actual de estos temas en el broker ignorando
    // los temas que no existen
    Collection <TopicWithOffset> endOffsets(Collection <String> topics) throws RemoteException;

    // obtiene todos los mensajes no leídos de los temas suscritos
    Map<String, List<byte []>> poll(Collection <TopicWithOffset> topics) throws RemoteException;

}
