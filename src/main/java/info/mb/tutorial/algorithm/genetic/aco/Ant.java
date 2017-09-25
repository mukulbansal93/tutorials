package info.mb.tutorial.algorithm.genetic.aco;

/**
 * 
 * @author MBansal
 *
 */
public class Ant {

	protected int noOfCities;
	protected int traversalOrder[];
	protected boolean visitedCities[];

	public Ant(int trailLength) {
		this.noOfCities = trailLength;
		this.traversalOrder = new int[trailLength];
		this.visitedCities = new boolean[trailLength];
	}

	protected void visitCity(int currentIndex, int city) {
		traversalOrder[currentIndex + 1] = city;
		visitedCities[city] = true;
	}

	protected boolean visited(int i) {
		return visitedCities[i];
	}

	protected double traversalCost(double cityMap[][]) {
		double length = cityMap[traversalOrder[noOfCities - 1]][traversalOrder[0]];
		for (int i = 0; i < noOfCities - 1; i++) {
			length += cityMap[traversalOrder[i]][traversalOrder[i + 1]];
		}
		return length;
	}

	protected void clear() {
		for (int i = 0; i < noOfCities; i++)
			visitedCities[i] = false;
	}

}
