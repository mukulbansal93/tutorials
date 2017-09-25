package info.mb.tutorial.algorithm.genetic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * A simple example of ACO used to solve the Traveling Salesman Problem.
 * 
 * @author MBansal
 *
 */
public class AntColonyOptimization {

	private static Random random = new Random();

	/**
	 * Original number of trails at the start of the simulation.
	 */
	private static double initialPheromoneLevels = 1.0;
	/**
	 * Importance of the pheromone trail.
	 */
	private static double alpha = 2;
	/**
	 * Heuristic information, distance priority in our case. Generally beta>alpha
	 */
	private static double beta = 10;
	/**
	 * The percent of how much the pheromone is evaporating in every iteration.
	 */
	private static double evaporationRate = 0.5;
	/**
	 * Information about the total amount of pheromone left on the trail by each
	 * Ant. It is a parameter to adjust the amount of pheromone deposited, typically
	 * it would be set to 1 but could be set to any random value.
	 */
	private static double Q = 200;
	/**
	 * Number of ants to be used.
	 */
	private static double antFactor = 0.8;
	/**
	 * Factor of randomness to introduce. Between 0 and 1.
	 */
	private static double randomFactor = 0.01;
	/**
	 * 
	 */
	private static int numberOfSimulations = 3;
	/**
	 * Number of iterations.
	 */
	private static int maxIterationsPerSimulation = 1000;

	private List<Ant> ants = new ArrayList<>();
	private int numberOfCities;
	private int numberOfAnts;
	private double cityMap[][];
	private double pheromoneLevels[][];
	private double probabilities[];
	private int currentIndex;
	private int[] bestTraversalOrder;
	private double bestTraversalLength;

	public static void main(String... s) {

		AntColonyOptimization aco = new AntColonyOptimization();

		// initialize
		int noOfCities = 3;
		aco.initParams(noOfCities);

		// iterate
		IntStream.rangeClosed(1, numberOfSimulations).forEach(attempt -> {
			aco.setupAnts();
			aco.clearPheromoneTrails();
			System.out.println("Attempt #" + attempt);
			IntStream.range(0, AntColonyOptimization.maxIterationsPerSimulation).forEach(i -> {
				aco.moveAnts();
				aco.globalPheromoneUpdate();
				aco.updateBestTraversal();
			});
			// show result
			aco.displayResult();
		});

	}

	/**
	 * 
	 * Initialize the ACO algorithm.
	 */
	private void initParams(int numberOfCities) {
		this.cityMap = generateRandomMatrix(numberOfCities);
		this.numberOfCities = numberOfCities;
		this.numberOfAnts = (int) (numberOfCities * antFactor);
		this.pheromoneLevels = new double[numberOfCities][numberOfCities];
		this.probabilities = new double[numberOfCities];
		IntStream.range(0, numberOfAnts).forEach(i -> this.ants.add(new Ant(numberOfCities)));
	}

	/**
	 * Generate initial solution
	 */
	public double[][] generateRandomMatrix(int numberOfCities) {
		double[][] randomMatrix = new double[numberOfCities][numberOfCities];
		// System.out.println("Initial Graph Matrix-");
		IntStream.range(0, numberOfCities).forEach(i -> {
			IntStream.range(0, numberOfCities).forEach(j -> {
				randomMatrix[i][j] = Math.abs(random.nextInt(100) + 1);
			});
		});
		// DISPLAY GENERATED MAP MATRIX
		// IntStream.range(0, numberOfCities).forEach(i -> {
		// System.out.println(Arrays.toString(randomMatrix[i]));
		// });
		return randomMatrix;
	}

	/**
	 * Setup ants at starting positions.
	 */
	private void setupAnts() {
		ants.forEach(ant -> {
			ant.clear();
			ant.visitCity(-1, random.nextInt(numberOfCities));
		});
		currentIndex = 0;
	}

	/**
	 * Move each ant to construct a new trail sequence of visited cities.
	 */
	private void moveAnts() {
		IntStream.range(currentIndex, numberOfCities - 1).forEach(i -> {
			ants.forEach(ant -> {
				ant.visitCity(currentIndex, selectNextCity(ant));
			});
			currentIndex++;
		});
	}

	/**
	 * Main part of the algorithm which decides the city to visited next by an ant.
	 * 
	 */
	private int selectNextCity(Ant ant) {

		// SOME RANDOM LOGIC TO ACCOUNT FOR RANDOMNESS IN ANT's BEHAVIOR. IT COULD BE AS
		// SIMPLE AS COMPARING A RANDOM VALUE WITH RANDOM FACTOR.
		int t = random.nextInt(numberOfCities);
		if (random.nextDouble() < randomFactor) {
			if (!ant.visited(t))
				return t;
		}

		calculateCityProbabilities(ant);

		double maxProbability = 0.0;
		int maxProbabilityCity = -1;

		for (int i = 0; i < numberOfCities; i++) {
			if (probabilities[i] >= maxProbability && probabilities[i] != 0.0) {
				maxProbabilityCity = i;
			}
		}

		return maxProbabilityCity;
	}

	/**
	 * Calculate the probabilites of the cities in order to find the city which is
	 * to be visited next by an ant.
	 */
	public void calculateCityProbabilities(Ant ant) {
		int i = ant.traversalOrder[currentIndex];
		double pheromone = 0.0;
		for (int l = 0; l < numberOfCities; l++) {
			if (!ant.visited(l)) {
				pheromone += Math.pow(pheromoneLevels[i][l], alpha) * Math.pow(cityMap[i][l], beta);
			}
		}
		for (int j = 0; j < numberOfCities; j++) {
			if (ant.visited(j)) {
				probabilities[j] = 0.0;
			} else {
				double numerator = Math.pow(pheromoneLevels[i][j], alpha) * Math.pow(cityMap[i][j], beta);
				probabilities[j] = numerator / pheromone;
			}
		}
	}

	/**
	 * Update pheromone levels of the graph. We are doing this after one complete
	 * iteration of the graph i.e. one complete construction of a soltuion by every
	 * ant. Ideally this should be done whenever any ant makes a move or a certain
	 * time period has been elapsed.
	 */
	private void globalPheromoneUpdate() {
		// EVAPORATING GLOBAL PHEROMONE LEVELS
		for (int i = 0; i < numberOfCities; i++) {
			for (int j = 0; j < numberOfCities; j++) {
				pheromoneLevels[i][j] *= evaporationRate;
			}
		}
		for (Ant a : ants) {
			// CONTRIBUTING TO GLOBAL PHEROMONE LEVELS
			double contribution = Q / a.traversalCost(cityMap);
			pheromoneLevels[a.traversalOrder[numberOfCities - 1]][a.traversalOrder[0]] += contribution;
			// UPDATING PHEROMONE LEVELS OF TRAVERSED PATHS
			for (int i = 0; i < numberOfCities - 1; i++) {
				pheromoneLevels[a.traversalOrder[i]][a.traversalOrder[i + 1]] += contribution;
			}
		}
	}

	/**
	 * Update the best traversal order after each iteration.
	 */
	private void updateBestTraversal() {
		if (bestTraversalOrder == null) {
			bestTraversalOrder = ants.get(0).traversalOrder;
			bestTraversalLength = ants.get(0).traversalCost(cityMap);
		}
		for (Ant a : ants) {
			if (a.traversalCost(cityMap) < bestTraversalLength) {
				bestTraversalLength = a.traversalCost(cityMap);
				bestTraversalOrder = a.traversalOrder.clone();
			}
		}
	}

	/**
	 * Does what it says.
	 */
	private void displayResult() {
		System.out.println("Best tour length: " + (bestTraversalLength - numberOfCities));
		System.out.println("Best tour order: " + Arrays.toString(bestTraversalOrder));
	}

	/**
	 * Initialize trails to initial level of pheromones before each simulation
	 */
	private void clearPheromoneTrails() {
		IntStream.range(0, numberOfCities).forEach(i -> {
			IntStream.range(0, numberOfCities).forEach(j -> {
				pheromoneLevels[i][j] = initialPheromoneLevels;
			});
		});
	}

}
