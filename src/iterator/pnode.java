package iterator; 

import global.*;
import bigt.Map;
import bufmgr.*;
import diskmgr.*;
import heap.*;

/**
 * A structure describing a tuple.
 * include a run number and the tuple
 */
public class pnode {
  /** which run does this tuple belong */
  public int     run_num;

  /** the tuple reference */
  public Map   tuple;

  /**
   * class constructor, sets <code>run_num</code> to 0 and <code>tuple</code>
   * to null.
   */
  public pnode() 
  {
    run_num = 0;  // this may need to be changed
    tuple = null; 
  }
  
  /**
   * class constructor, sets <code>run_num</code> and <code>tuple</code>.
   * @param runNum the run number
   * @param t      the tuple
   */
  public pnode(int runNum, Map t) 
  {
    run_num = runNum;
    tuple = t;
  }
  
}

