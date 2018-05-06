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
	   ResourceInformation[] minShareInfo1 = s1.getMinShare().getResources();
	   ResourceInformation[] minShareInfo2 = s2.getMinShare().getResources();
	  
       double[] ourFairness1 = calculateOurFairness(request1, s1.getWeight(), minShareInfo1, null);
       double[] ourFairness2 = calculateOurFairness(request2, s2.getWeight(), minShareInfo2, null);
       
       double temperature=0 ;
       
       int fitness1 = calculateFitness(request1, s1.getWeight(), clusterCapacity);
       int fitness2 = calculateFitness(request2, s2.getWeight(), clusterCapacity);
       // queue is needy if req_mem < minshare_mem or req_vcore < minshare_vcore 
       boolean s1Needy = (request1[Resource.MEMORY_INDEX].getValue() < minShareInfo1[Resource.MEMORY_INDEX].getValue()) 
    		          || (request1[Resource.VCORES_INDEX].getValue() < minShareInfo1[Resource.VCORES_INDEX].getValue()) ;
       
       boolean s2Needy = (request2[Resource.MEMORY_INDEX].getValue() < minShareInfo2[Resource.MEMORY_INDEX].getValue()) 
		          || (request2[Resource.VCORES_INDEX].getValue() < minShareInfo2[Resource.VCORES_INDEX].getValue()) ;
       
    	     
       
       int res = 0;
       
       if (s1Needy && !s2Needy) {
    	   res = -1 ; 
    	  
       }else if (!s1Needy && s2Needy) {
    	   res = 1 ; 
       }else if (s1Needy && s2Needy) {
    	   
     //  res = compareFitness(fitness1, fitness2) ;
    	 res = compareSimulatedAnnealing(temperature) ;
       }
       
       return res;
   }
 //***********************************************************************      
  

   @VisibleForTesting
   int calculateFitness(ResourceInformation[] request,
       float weight, ResourceInformation[] clusterCapacity) {
     int fitness= ((int) request[Resource.MEMORY_INDEX].getValue()*
    		 (int) clusterCapacity[Resource.MEMORY_INDEX].getValue()) +
    		 ((int) request[Resource.VCORES_INDEX].getValue()*
    	    		 (int) clusterCapacity[Resource.VCORES_INDEX].getValue()) ;

    

     return fitness;
   }
   
   //**********************************************************************
   int compareFitness(float fitness1 , float fitness2) {
	   int ret = 0;
	   
	   if (fitness1 > fitness2) {
		   ret = 1;
	   }else if (fitness1 < fitness2) {
		   ret = -1 ;
	   }
	   
	   return ret ;
   }
   
 //**********************************************************************
   int compareSimulatedAnnealing(double temperature ) {
	   int ret = 0;
	  
	   
	   return ret ;
   }
   
	//***********************************************************************
     double [] calculateOurFairness(ResourceInformation[] usage,
	        float weight, ResourceInformation[] minshare, double[] shares) {
	      float dominant;

	      shares[Resource.MEMORY_INDEX] =
	          ((double) usage[Resource.MEMORY_INDEX].getValue()) /
	          minshare[Resource.MEMORY_INDEX].getValue() * weight;
	      shares[Resource.VCORES_INDEX] =
	          ((double) usage[Resource.VCORES_INDEX].getValue()) /
	          minshare[Resource.VCORES_INDEX].getValue() * weight;
	      

	      return shares;
   }
   }

	 static class TestResourceCalculator extends ResourceCalculator{
		 public TestResourceCalculator() {
		  }

		@Override
		public int compare(Resource clusterResource, Resource lhs, Resource rhs, boolean singleType) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public long computeAvailableContainers(Resource available, Resource required) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public Resource multiplyAndNormalizeUp(Resource r, double by, Resource stepFactor) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Resource multiplyAndNormalizeDown(Resource r, double by, Resource stepFactor) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Resource normalize(Resource r, Resource minimumResource, Resource maximumResource, Resource stepFactor) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Resource roundUp(Resource r, Resource stepFactor) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Resource roundDown(Resource r, Resource stepFactor) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public float divide(Resource clusterResource, Resource numerator, Resource denominator) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public boolean isInvalidDivisor(Resource r) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public float ratio(Resource a, Resource b) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public Resource divideAndCeil(Resource numerator, int denominator) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Resource divideAndCeil(Resource numerator, float denominator) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean fitsIn(Resource smaller, Resource bigger) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isAnyMajorResourceZero(Resource resource) {
			// TODO Auto-generated method stub
			return false;
		}
	 }
}
