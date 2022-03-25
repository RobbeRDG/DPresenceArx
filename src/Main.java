import org.deidentifier.arx.*;
import org.deidentifier.arx.aggregates.HierarchyBuilder;
import org.deidentifier.arx.aggregates.HierarchyBuilderDate;
import org.deidentifier.arx.criteria.DPresence;
import org.deidentifier.arx.metric.Metric;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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

        //Read in the population data
        Data.DefaultData populationData = Data.create();
        populationData.add("person_id","gender_concept_id","birth_datetime","race_concept_id");
        try (BufferedReader br = new BufferedReader(new FileReader(populationDataSetPath))) {
            br.readLine();
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(";");
                populationData.add(values);
            }
        }

        //Read in the research data
        Data.DefaultData researchData = Data.create();
        researchData.add("person_id","gender_concept_id","birth_datetime","race_concept_id");
        try (BufferedReader br = new BufferedReader(new FileReader(researchDataSetPath))) {
            br.readLine();
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(";");
                researchData.add(values);
            }
        }

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


        //Set the attribute types for the research data
        researchData.getDefinition().setAttributeType("person_id", AttributeType.IDENTIFYING_ATTRIBUTE);
        researchData.getDefinition().setAttributeType("gender_concept_id", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        researchData.getDefinition().setAttributeType("birth_datetime", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
        researchData.getDefinition().setAttributeType("race_concept_id", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);

        //Set the attribute data types for the research data
        researchData.getDefinition().setDataType("person_id", DataType.INTEGER);
        researchData.getDefinition().setDataType("gender_concept_id", DataType.INTEGER);
        researchData.getDefinition().setDataType("birth_datetime", DataType.createDate("yyyy-MM-dd hh:mm:ss"));
        researchData.getDefinition().setDataType("race_concept_id", DataType.INTEGER);


        //Get the different values of the birthtime attribute in the research data
        int attributeIndexForBirthDateTime = researchData.getHandle().getColumnIndexOf("birth_datetime");
        String[] birthDateTimeValues = researchData.getHandle().getDistinctValues(attributeIndexForBirthDateTime);

        //Add hierarchies to the research data
        researchData.getDefinition().setHierarchy("gender_concept_id", hierarchyForGenderConceptId);
        hierarchyBuilderForDates.prepare(birthDateTimeValues);
        researchData.getDefinition().setHierarchy("birth_datetime", hierarchyBuilderForDates.build());
        researchData.getDefinition().setHierarchy("race_concept_id", hierarchyForRaceConceptId);

        //Create the research sample subset
        DataSubset researchSubset = DataSubset.create(populationData,researchData);

        //Set the arx configuration
        ARXConfiguration config = ARXConfiguration.create();
        config.addPrivacyModel(new DPresence(0.0d,0.1d, researchSubset));
        config.setSuppressionLimit(0.1d);
        config.setQualityModel(Metric.createLossMetric(Metric.AggregateFunction.ARITHMETIC_MEAN));
        config.setAttributeWeight("gender_concept_id", 0.5d);
        config.setAttributeWeight("birth_datetime", 0.5d);
        config.setAttributeWeight("race_concept_id", 0.5d);

        //Create an anonymizer instance
        ARXAnonymizer anonymizer = new ARXAnonymizer();

        //Anonymize
        ARXResult result = anonymizer.anonymize(researchData, config);

        //Get the lattice of all considered solutions
        ARXLattice.ARXNode[][] nodes = result.getLattice().getLevels();

        //Get the data handle
        DataHandle dataHandle = result.getOutput();

        //Print the loss
        System.out.println(dataHandle.getTransformation().getLowestScore() + " " + dataHandle.getTransformation().getHighestScore());

        //Write the anonymized data
        dataHandle.save(basePath + "out/out.csv", ';');
    }

}
