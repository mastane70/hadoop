package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy.DominantResourceFairnessComparator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies.DominantResourceFairnessPolicy.DominantResourceFairnessComparator2;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
public class TestPolicy extends SchedulingPolicy {
   
	public static final String NAME = "TEST";
	private static final TestComparator2 COMPARATOR =
		      new TestComparator2();
	//private static final TestCalculator CALCULATOR =
	//	      new TestCalculator();
	
	@Override
	public ResourceCalculator getResourceCalculator() {
		// TODO Auto-generated method stub
		//return CALCULATOR;
		return null;
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return NAME;
	}

	@Override
	public Comparator<Schedulable> getComparator() {
		// TODO Auto-generated method stub
		return COMPARATOR;
	}

	@Override
	public void computeShares(Collection<? extends Schedulable> schedulables, Resource totalResources) {
		// TODO Auto-generated method stub
		for (ResourceInformation info: ResourceUtils.getResourceTypesArray()) {
		      ComputeFairShares.computeShares(schedulables, totalResources,
		          info.getName());
		    }
	}

	@Override
	public void computeSteadyShares(Collection<? extends FSQueue> queues, Resource totalResources) {
		// TODO Auto-generated method stub
		 for (ResourceInformation info: ResourceUtils.getResourceTypesArray()) {
		      ComputeFairShares.computeSteadyShares(queues, totalResources,
		          info.getName());
		    }
		
	}

	@Override
	public boolean checkIfUsageOverFairShare(Resource usage, Resource fairShare) {
		// TODO Auto-generated method stub
		return !Resources.fitsIn(usage, fairShare);
	}

	@Override
	public Resource getHeadroom(Resource queueFairShare, Resource queueUsage, Resource maxAvailable) {
		// TODO Auto-generated method stub
		long queueAvailableMemory =
		        Math.max(queueFairShare.getMemorySize() - queueUsage.getMemorySize(), 0);
		    int queueAvailableCPU =
		        Math.max(queueFairShare.getVirtualCores() - queueUsage
		            .getVirtualCores(), 0);
		    Resource headroom = Resources.createResource(
		        Math.min(maxAvailable.getMemorySize(), queueAvailableMemory),
		        Math.min(maxAvailable.getVirtualCores(),
		            queueAvailableCPU));
		    return headroom;
	}
	
	@Override
	  public void initialize(FSContext fsContext) {
	    COMPARATOR.setFSContext(fsContext);
	  }
	
	public abstract static class TestComparator
    implements Comparator<Schedulable> {
  protected FSContext fsContext;

  public void setFSContext(FSContext fsContext) {
    this.fsContext = fsContext;
  }

  /**
   * This method is used when apps are tied in fairness ratio. It breaks
   * the tie by submit time and job name to get a deterministic ordering,
   * which is useful for unit tests.
   *
   * @param s1 the first item to compare
   * @param s2 the second item to compare
   * @return &lt; 0, 0, or &gt; 0 if the first item is less than, equal to,
   * or greater than the second item, respectively
   */
  protected int compareAttribrutes(Schedulable s1, Schedulable s2) {
    int res = (int) Math.signum(s1.getStartTime() - s2.getStartTime());

    if (res == 0) {
      res = s1.getName().compareTo(s2.getName());
    }

    return res;
  }
}

	
	 static class TestComparator2
     extends TestComparator {
   @Override
   public int compare(Schedulable s1, Schedulable s2) {
	//**************************my code ********************************   
	   ResourceInformation[] request1 =
		          s1.getResourceUsage().getResources();
	   ResourceInformation[] request2 =
		          s2.getResourceUsage().getResources();
	   ResourceInformation[] clusterCapacity =
		          fsContext.getClusterResource().getResources();
       double[] shares1 = new double[2];
       double[] shares2 = new double[2];
       int fitness1 = calculateFitness(request1, s1.getWeight(), clusterCapacity);
       int fitness2 = calculateFitness(request2, s2.getWeight(), clusterCapacity);
       boolean s1Needy =
       boolean s2Needy = 
 //***********************************************************************      
     ResourceInformation[] resourceInfo1 =
         s1.getResourceUsage().getResources();
     ResourceInformation[] resourceInfo2 =
         s2.getResourceUsage().getResources();
     ResourceInformation[] minShareInfo1 = s1.getMinShare().getResources();
     ResourceInformation[] minShareInfo2 = s2.getMinShare().getResources();
     ResourceInformation[] clusterInfo =
         fsContext.getClusterResource().getResources();
     double[] shares1 = new double[2];
     double[] shares2 = new double[2];

     int dominant1 = calculateClusterAndFairRatios(resourceInfo1,
         s1.getWeight(), clusterInfo, shares1);
     int dominant2 = calculateClusterAndFairRatios(resourceInfo2,
         s2.getWeight(), clusterInfo, shares2);

     // A queue is needy for its min share if its dominant resource
     // (with respect to the cluster capacity) is below its configured min
     // share for that resource
     boolean s1Needy = resourceInfo1[dominant1].getValue() <
         minShareInfo1[dominant1].getValue();
     boolean s2Needy = resourceInfo1[dominant2].getValue() <
         minShareInfo2[dominant2].getValue();

     int res;

     if (!s2Needy && !s1Needy) {
       res = (int) Math.signum(shares1[dominant1] - shares2[dominant2]);

       if (res == 0) {
         // Because memory and CPU are indices 0 and 1, we can find the
         // non-dominant index by subtracting the dominant index from 1.
         res = (int) Math.signum(shares1[1 - dominant1] -
             shares2[1 - dominant2]);
       }
     } else if (s1Needy && !s2Needy) {
       res = -1;
     } else if (s2Needy && !s1Needy) {
       res = 1;
     } else {
       double[] minShares1 =
           calculateMinShareRatios(resourceInfo1, minShareInfo1);
       double[] minShares2 =
           calculateMinShareRatios(resourceInfo2, minShareInfo2);

       res = (int) Math.signum(minShares1[dominant1] - minShares2[dominant2]);

       if (res == 0) {
         res = (int) Math.signum(minShares1[1 - dominant1] -
             minShares2[1 - dominant2]);
       }
     }

     if (res == 0) {
       res = compareAttribrutes(s1, s2);
     }

     return res;
   }


   @VisibleForTesting
   int calculateFitness(ResourceInformation[] request,
       float weight, ResourceInformation[] clusterCapacity) {
     int fitness= ((int) request[Resource.MEMORY_INDEX].getValue()*
    		 (int) clusterCapacity[Resource.MEMORY_INDEX].getValue()) +
    		 ((int) request[Resource.VCORES_INDEX].getValue()*
    	    		 (int) clusterCapacity[Resource.VCORES_INDEX].getValue()) ;

    

     return fitness;
   }
   
	
	 }
}
