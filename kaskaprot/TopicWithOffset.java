// Clase usada por el servicio para encapsular la información correspondiente
// a un tema y un offset asociado al mismo.
// NO MODIFICAR

package kaskaprot;
import java.io.Serializable;

public class TopicWithOffset implements Serializable {
    public static final long serialVersionUID=1234567890L;
    String topic;
    int offset;
    public TopicWithOffset(String t, int o) {
        topic = t;
        offset = o;
    }
    public String getTopic() {
        return topic;
    }
    public int getOffset() {
        return offset;
    }
    // devuelve el offset y luego lo incrementa
    public int postIncOffset() {
        return offset++;
    }
    public void setOffset(int newOffset) {
        offset = newOffset;
    }
}
