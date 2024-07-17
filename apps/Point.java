// Clase definida por el usuario que se usa en el ejemplo Emisor/Receptor
package apps;
import java.io.Serializable;
import java.util.Random;

// Debe ser serializable para poderse enviar
public class Point implements Serializable {
    public static final long serialVersionUID=1234567890L;
    int x;
    int y;
    public Point (int x, int y) {
        this.x = x;
        this.y = y;
    }
    public Point () {
        this.x = new Random().nextInt();
        this.y = new Random().nextInt();
    }
    public int getX() {
        return x;
    }
    public int getY() {
        return y;
    }
    public String toString() {
        return "(" + x + "," + y + ")";
    }
}
