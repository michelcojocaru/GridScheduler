package distributed.systems.core;

import java.util.Observer;

/**
 * Created by michelcojocaru on 23/11/2017.
 */
public interface Observable {

    public void addObserver(Observer o);

    public void removeObserver(Observer o);

    public void notifyObserver();
}
