package eu.stratosphere.peel.analyser.controller;

import eu.stratosphere.peel.analyser.exception.PeelAnalyserException;
import eu.stratosphere.peel.analyser.model.Experiment;
import eu.stratosphere.peel.analyser.model.ExperimentRun;
import eu.stratosphere.peel.analyser.model.ExperimentSuite;
import eu.stratosphere.peel.analyser.model.System;
import eu.stratosphere.peel.analyser.util.HibernateUtil;
import eu.stratosphere.peel.analyser.util.ORM;
import eu.stratosphere.peel.analyser.util.QueryParameter;
import org.json.JSONObject;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Fabian on 08.11.14.
 */
class ParserManagerHelper {
  public static final ORM orm = HibernateUtil.getORM();

  /**
   * gets the system saved in the database by the systemName
   *
   * @param systemName the name of the system
   * @param version    of the system
   * @return the system
   */
  protected static System getSystem(String systemName, String version) {
    List<System> systemList;
    String query = "from System where name = :systemName and version = :version";
    orm.beginTransaction();
    systemList = orm.executeQuery(System.class, query,
		    new QueryParameter("systemName", systemName),
		    new QueryParameter("version", version));
    orm.commitTransaction();
    if (systemList.iterator().hasNext()) {
      return systemList.iterator().next();
    } else {
      return null;
    }
  }

  /**
   * saves a system as described by the state.json
   *
   * @param stateJsonObj the state.json as a JSONObject
   * @return the created system
   */
  protected static System saveSystem(JSONObject stateJsonObj) {
    orm.beginTransaction();

    eu.stratosphere.peel.analyser.model.System system = new System();
    system.setName(stateJsonObj.getString("runnerName"));
    system.setVersion(stateJsonObj.getString("runnerVersion"));
    orm.save(system);

    orm.commitTransaction();
    return system;
  }

  /**
   * this method will search for the experimentSuite based on the name
   *
   * @param experimentSuiteName the name of the experimentSuite
   * @return the experimentSuite or null if not found
   */
  protected static ExperimentSuite getExperimentSuite(
		  String experimentSuiteName) {
    String query = "from ExperimentSuite where name = :experimentSuiteName";
    orm.beginTransaction();
    List<ExperimentSuite> experimentSuite = orm.executeQuery(
		    ExperimentSuite.class,
		    query, new QueryParameter("experimentSuiteName",
				    experimentSuiteName));
    orm.commitTransaction();
    if (experimentSuite.iterator().hasNext()) {
      return experimentSuite.iterator().next();
    } else {
      return null;
    }
  }

  /**
   * if no experimentSuite was found this method will save the experimentSuite based on the
   * information of the state.json file
   *
   * @param stateJsonObj The JSONObject of the state.json that describes the ExperimentRun
   * @return the experimentSuite that was created
   */
  protected static ExperimentSuite saveExperimentSuite(
		  JSONObject stateJsonObj) {
    orm.beginTransaction();

    ExperimentSuite experimentSuite = new ExperimentSuite();
    experimentSuite.setName(stateJsonObj.getString("suiteName"));
    orm.save(experimentSuite);

    orm.commitTransaction();
    return experimentSuite;
  }

  /**
   * This method parses the JSONObject representing the state.json file and will search in the
   * database for the experiment of the experimentRun as described by the state.json.
   *
   * @param stateJsonObj The JSONObject of the state.json that describes the ExperimentRun
   * @return the experiment of the experimentRun that was described by the state.json
   * @throws PeelAnalyserException
   */
  protected static Experiment getExperiment(JSONObject stateJsonObj)
		  throws PeelAnalyserException {
    String query = "select experiment from Experiment as experiment join experiment.system as system join experiment.experimentSuite as experimentSuite where experiment.name = :experimentName AND system.name = :systemName AND experimentSuite.name = :experimentSuiteName";
    orm.beginTransaction();
    List<Experiment> experiment = orm.executeQuery(Experiment.class, query,
		    new QueryParameter("experimentName",
				    getExperimentName(stateJsonObj)),
		    new QueryParameter("systemName",
				    stateJsonObj.get("runnerName")),
		    new QueryParameter("experimentSuiteName",
				    stateJsonObj.get("suiteName")));
    orm.commitTransaction();
    if (experiment.iterator().hasNext()) {
      return experiment.iterator().next();
    } else {
      return null;
    }
  }

  /**
   * Will save a experiment based on the experimentName, the experimentSuite of this experiment
   * and the system this experiment was executed on.
   *
   * @param experimentName The name of the experiment
   * @param suite          The suite of the experiment
   * @param system         The system this experiment was executed on
   * @return The created Experiment (is saved to database as well)
   */
  protected static Experiment saveExperiment(String experimentName,
		  ExperimentSuite suite, System system) {
    orm.beginTransaction();

    Experiment experiment = new Experiment();
    experiment.setName(experimentName);
    experiment.setExperimentSuite(suite);
    suite.getExperimentSet().add(experiment);
    experiment.setSystem(system);
    system.getExperimentSet().add(experiment);
    orm.save(experiment);

    orm.commitTransaction();
    return experiment;
  }

  /**
   * This method will parse the JSONObject representing the state.json file and will
   * return the run-number of the ExperimentRun that is described by the state.json
   *
   * @param stateJsonObj
   * @return
   * @throws PeelAnalyserException
   */
  private static int parseExperimentRunCount(JSONObject stateJsonObj)
		  throws PeelAnalyserException {
    Pattern pattern = Pattern.compile("(?<=.run)[0-9]+");
    Matcher matcher = pattern.matcher(stateJsonObj.getString("name"));
    if (matcher.find()) {
      String runCountString = matcher.group();
      return Integer.valueOf(runCountString);
    } else {
      throw new PeelAnalyserException("No match for ExperimentRunCount found");
    }
  }

  /**
   * This method will parse the JSONObject representing the state.json file
   * and will return the name of the ExperimentRun
   *
   * @param stateJsonObj
   * @return
   * @throws PeelAnalyserException
   */
  protected static String getExperimentName(JSONObject stateJsonObj)
		  throws PeelAnalyserException {
    Pattern pattern = Pattern.compile("[A-z0-9.]+(?=.run[0-9]+)");
    String input = stateJsonObj.getString("name");
    Matcher matcher = pattern.matcher(input);
    if (matcher.find()) {
      return matcher.group();
    } else {
      throw new PeelAnalyserException("No match for ExperimentName found");
    }
  }

    /**
     * this method checks if the given filename is the jobmanager logfile
     * @param filename of the file you want to know if it's the jobmanager logfile
     * @return true if this is the jobmanager logfile, false if not
     */
    protected static boolean isJobmanager(String filename, String system){
        Pattern pattern = null;
        if(system.equals("flink")) {
            pattern = Pattern.compile("([A-z0-9-])+(-jobmanager-)([A-z0-9-])+(?=.log)");
        } else if(system.equals("spark")){
            pattern = Pattern.compile("app-*");
        }
        Matcher matcher = pattern.matcher(filename);
        return matcher.find();
    }
    Matcher matcher = pattern.matcher(filename);
    return matcher.find();
  }

  /**
   * this method finds the jobmanager file in the logs directory of a given ExperimentRun directory
   *
   * @param experimentRunFile the directory of the experimentRun
   * @return the logfile of the jobmanager
   */
  protected static File findLogFile(File experimentRunFile, String system) {
    File[] files = experimentRunFile.listFiles();
    File logs = null;
    for (File file : files) {
      if (file.getName().equals("logs")) {
	logs = file;
      }
    }
    File[] logFiles = logs.listFiles();
    for (File logFile : logFiles) {
      if (isJobmanager(logFile.getName(), system)) {
	return logFile;
      }
    }
    return null;
  }

  /**
   * This method returns the System of a ExperimentRun
   *
   * @param experimentRun
   * @return Systemname
   */
  protected static String getSystemOfExperimentRun(
		  ExperimentRun experimentRun) {
    System system = experimentRun.getExperiment().getSystem();
    return system.getName().toLowerCase();
  }

  /**
   * Saves a ExperimentRun into database based on the state.json file
   *
   * @param experiment   The experiment of the experimentRun
   * @param stateJsonObj The JSONObject of the state.json that describes the ExperimentRun
   * @return The created ExperimentRun
   * @throws PeelAnalyserException
   */
  protected static ExperimentRun saveExperimentRun(Experiment experiment,
		  JSONObject stateJsonObj) throws PeelAnalyserException {
    orm.beginTransaction();
    ExperimentRun experimentRun = new ExperimentRun();
    experimentRun.setExperiment(experiment);
    experimentRun.setRun(
		    ParserManagerHelper.parseExperimentRunCount(stateJsonObj));
    experiment.getExperimentRunSet().add(experimentRun);
    orm.save(experimentRun);
    orm.update(experiment);
    orm.commitTransaction();
    return experimentRun;
  }

  /**
   * checks if a ExperimentRun has failed.
   *
   * @param stateJsonObj the state.json as a JSONObject
   * @return false if ExperimentRun finished correctly and true if ExperimentRun failed
   */
  protected static boolean isFailedExperimentRun(JSONObject stateJsonObj) {
    return !stateJsonObj.get("runExitCode").equals(0);
  }

  /**
   * sets the AverageExperimentRunTime of every Experiment (median and arithmetic average)
   * ExperimentRunTime is defined as FinishTime - SubmitTime
   */
  protected static void setAverageExperimentRunTimes() {
    orm.beginTransaction();
    List<Experiment> experimentList = orm.executeQuery(Experiment.class,
		    "from Experiment");

    for (Experiment experiment : experimentList) {
      long timeSum = 0;
      long[] experimentRunTime = new long[experiment.getExperimentRunSet()
		      .size()];
      int i = 0;
      for (ExperimentRun experimentRun : experiment.getExperimentRunSet()) {
	if (experimentRun.getFinished() != null
			&& experimentRun.getSubmitTime() != null) {
	  long runTime = experimentRun.getFinished().getTime()
			  - experimentRun.getSubmitTime().getTime();
	  timeSum += runTime;
	  experimentRunTime[i] = runTime;
	}
	i++;
      }
      long timeAvg = timeSum / experiment.getExperimentRunSet().size();
      Arrays.sort(experimentRunTime);
      long timeMedian = experimentRunTime[(i+1)/2];
      experiment.setMedianExperimentRunTime(timeMedian);
      experiment.setAverageExperimentRunTime(timeAvg);
      orm.update(experiment);
    }
    orm.commitTransaction();
  }

}
