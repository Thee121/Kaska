// Clase definida por el usuario que no se puede enviar al no ser serializable
package apps;
import java.util.Random;

// No es serializable: dará error si se intenta enviar
public class PointError {
    int x;
    int y;
    public PointError (int x, int y) {
        this.x = x;
        this.y = y;
    }
    public PointError () {
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
