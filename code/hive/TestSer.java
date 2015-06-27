import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


public class TestSer {
	public static void main (String[] str)  {
		Person p = new Person();
		p.first_name="Mark";
		p.last_name="Johnson";
		p.age=33;
	
		try {
      			FileOutputStream  fos = new FileOutputStream("person.ser");
      			ObjectOutputStream out = new ObjectOutputStream(fos);
      			out.writeObject(p);

      			out.close();
    		} catch (Exception ex) {
      			ex.printStackTrace();
    		}
    }
}
