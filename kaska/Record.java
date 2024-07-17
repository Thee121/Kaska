// Clase que define el contenedor de un mensaje que se usa en la
// interfaz de las aplicaciones incluyendo el nombre del tema, el offset
// y el contenido del mensaje, que será deserializado al ser accedido.
// NO MODIFICAR
package kaska;
import java.io.Serializable;
import java.io.IOException;
import java.lang.ClassCastException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

public class Record implements Serializable {
    public static final long serialVersionUID=1234567890L;
    String topic;
    int offset;
    byte [] message;
    public Record(String t, int o, byte [] m) {
        topic = t;
        offset = o;
        message = m;
    }
    public String getTopic() {
        return topic;
    }
    public int getOffset() {
        return offset;
    }
    public Object getMessage() { // deserializa el mensaje
        Object m=null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(message);
            ObjectInputStream ois = new ObjectInputStream(bis);
            m = ois.readObject();
        } catch (ClassNotFoundException|IOException e) {
            System.err.println("error en la deserialización " + e.toString());
        }
        return m;
    }
}
