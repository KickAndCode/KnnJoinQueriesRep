package pythiaHbase;
import java.util.ArrayList;
import java.util.NoSuchElementException;


public class AscendingHeapSort {
  ArrayList<DataDistance> kNN_List;

  int myroot = 0;
  public AscendingHeapSort( )
  {
    kNN_List = new ArrayList<DataDistance>();
  }

  public boolean isEmpty()
  {
    return kNN_List.size() == 0;
  }

  public int size()
  {
    return kNN_List.size();
  }

  public void addNewData(String path, double dis, int numDataItems, double[] vec, int indexID)
  {

    kNN_List.add(new DataDistance(path, dis, numDataItems, vec, indexID));

    upBable(size()-1);

  }


  public DataDistance removeMin()
  {
    if (isEmpty()) throw new NoSuchElementException();

    DataDistance root = kNN_List.get(0);

    kNN_List.set(0, kNN_List.get(kNN_List.size()-1));

    kNN_List.remove(kNN_List.size()-1);

    downBable(0);

    return root;
  }
  public void downBable(int index)
  {
    int child = 2* (index +1 ); // right child

    if (child >= size() || Double.compare(kNN_List.get(child - 1).getDistance(), kNN_List.get(child).getDistance())<0) child -=1;  // chose the largest child

    if (child >= size()) return;

    if (Double.compare(kNN_List.get(index).getDistance(), kNN_List.get(child).getDistance()) <= 0) return; // if parent is greater than child

    swap(index, child);

    downBable(child);

  }



  protected void upBable(int index)
  {
    if (index == 0) return;

    int parent = (index -1)/2;

    if ( Double.compare(kNN_List.get(parent).getDistance(), kNN_List.get(index).getDistance())<= 0) return;

    swap(parent, index);

    upBable(parent);

  }
  private void swap(int parent, int child)
  {
    DataDistance temp = kNN_List.get(parent);

    kNN_List.set(parent, kNN_List.get(child));

    kNN_List.set(child, temp);

  }

  public ArrayList<DataDistance> getClosestNN()
  {
    return this.kNN_List;
  }
}
