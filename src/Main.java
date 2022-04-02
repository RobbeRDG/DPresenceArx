import org.deidentifier.arx.*;
import org.deidentifier.arx.aggregates.HierarchyBuilder;
import org.deidentifier.arx.aggregates.HierarchyBuilderDate;
import org.deidentifier.arx.criteria.DPresence;
import org.deidentifier.arx.metric.Metric;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;

public class Main {
    public static void main(String[] args) throws IOException {
        //Define a file base path
        String basePath = System.getProperty("user.dir") + "/datasets/";

        //Define the list of research sample datasets
        String[] researchDataSetNames = {
                "people_who_died_from_corona"
        };

        //Set the paths for the population table and the research table
        String populationDataSetPath = basePath + "population.csv";
        String researchDataSetPath = basePath + researchDataSetNames[0] + ".csv";

        //Read in the population data using the CSV reader
        Data populationData = Data.create(populationDataSetPath, StandardCharsets.UTF_8,';');

        //Read in the research data using the CSV reader
        Data researchData = Data.create(researchDataSetPath,StandardCharsets.UTF_8,';');

        //Create the research sample subset
        DataSubset researchSubset = DataSubset.create(populationData,researchData);

        //Define the hierarchies for the attributes
        //Race
        AttributeType.Hierarchy.DefaultHierarchy hierarchyForRaceConceptId = AttributeType.Hierarchy.create();
        hierarchyForRaceConceptId.add("0","{0, 8527, 8515, 8516}");
        hierarchyForRaceConceptId.add("8527","{0, 8527, 8515, 8516}");
        hierarchyForRaceConceptId.add("8515","{0, 8527, 8515, 8516}");
        hierarchyForRaceConceptId.add("8516","{0, 8527, 8515, 8516}");

        //Birth Date
        HierarchyBuilder<Date> hierarchyBuilderForDates = HierarchyBuilderDate.create(
                DataType.createDate("yyyy-MM-dd hh:mm:ss"),
                HierarchyBuilderDate.Granularity.DAY_MONTH_YEAR,
                HierarchyBuilderDate.Granularity.MONTH_YEAR,
                HierarchyBuilderDate.Granularity.YEAR,
                HierarchyBuilderDate.Granularity.DECADE,
                HierarchyBuilderDate.Granularity.CENTURY
        );

        //Gender
        AttributeType.Hierarchy.DefaultHierarchy hierarchyForGenderConceptId = AttributeType.Hierarchy.create();
        hierarchyForGenderConceptId.add("8507", "{8507,8532}");
        hierarchyForGenderConceptId.add("8532", "{8507,8532}");

        // ALL settings need to be done on the population dataset

        //Set the attribute types for the research data
        populationData.getDefinition().setAttributeType("person_id", AttributeType.IDENTIFYING_ATTRIBUTE);
        populationData.getDefinition().setAttributeType("gender_concept_id", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        populationData.getDefinition().setAttributeType("birth_datetime", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        populationData.getDefinition().setAttributeType("race_concept_id", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);

        //Set the attribute data types for the research data
        populationData.getDefinition().setDataType("person_id", DataType.INTEGER);
        populationData.getDefinition().setDataType("gender_concept_id", DataType.INTEGER);
        populationData.getDefinition().setDataType("birth_datetime", DataType.createDate("yyyy-MM-dd hh:mm:ss"));
        populationData.getDefinition().setDataType("race_concept_id", DataType.INTEGER);

        //Add hierarchies to the research data
        populationData.getDefinition().setHierarchy("gender_concept_id", hierarchyForGenderConceptId);
        //builder does not need to be build first
        populationData.getDefinition().setHierarchy("birth_datetime", hierarchyBuilderForDates);
        populationData.getDefinition().setHierarchy("race_concept_id", hierarchyForRaceConceptId);

        //Set the arx configuration
        ARXConfiguration config = ARXConfiguration.create();
        config.addPrivacyModel(new DPresence(0.0d,0.1d, researchSubset));
        config.setSuppressionLimit(0.1d);
        config.setQualityModel(Metric.createLossMetric(Metric.AggregateFunction.ARITHMETIC_MEAN));
        // DEFAULT is 0.5
        //config.setAttributeWeight("gender_concept_id", 0.5d);
        //config.setAttributeWeight("birth_datetime", 0.5d);
        //config.setAttributeWeight("race_concept_id", 0.5d);

        //Create an anonymizer instance
        ARXAnonymizer anonymizer = new ARXAnonymizer();

        //Anonymize
        ARXResult result = anonymizer.anonymize(populationData, config);

        //Get the lattice of all considered solutions
        //ARXLattice.ARXNode[][] nodes = result.getLattice().getLevels();
        System.out.println(Arrays.toString(result.getGlobalOptimum().getTransformation()));

        //Get the data handle
        DataHandle dataHandle = result.getOutput();

        //Print the loss
        System.out.println(dataHandle.getTransformation().getLowestScore() + " " + dataHandle.getTransformation().getHighestScore());

        //Write the anonymized data
        //Writes the view of the datahandle (AKA the sample/researchsubset)
        dataHandle.getView().save(basePath + "out/out.csv", ';');
    }

}
