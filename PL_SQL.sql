CREATE OR REPLACE PACKAGE LS_CCO_MT
AS
    PROCEDURE INIT (EXECUTIONGUID             IN     RAW,
                    EXECUTIONSTARTTIMESTAMP   IN     TIMESTAMP,
                    EXECNO                    IN     NUMBER,
                    FIRSTEXECUTION            IN     NUMBER,
                    LASTEXECUTION             IN     NUMBER,
                    EXECUTIONPROFILE          IN     XMLTYPE,
                    GENERICMETADATA           IN     XMLTYPE,
                    KPICMMAPPINGS             IN     XMLTYPE,
                    MACHINEOS                 IN     VARCHAR2,
                    AUTHINFO                  IN     VARCHAR2,
                    OPERATIONTYPE             IN     NUMBER,
                    EXECUTIONTIME             IN     TIMESTAMP,
                    RESULTCODE                   OUT NUMBER,
                    OBSERVATIONPERIODS        IN     XMLTYPE,
                    EXECUTIONPARAMS           IN     XMLTYPE);
 
    FUNCTION BEARINGDIFFERENCE (BEARING1 IN NUMBER, BEARING2 IN NUMBER)
        RETURN NUMBER
        DETERMINISTIC
        PARALLEL_ENABLE;
END LS_CCO_MT;

CREATE OR REPLACE PACKAGE BODY LS_CCO_MT
AS
   /******************************************************************************
      NAME:       LITESON_CCO_MT
      PURPOSE:

      REVISIONS:
      Ver        Date        Author           Description
      ---------  ----------  ---------------  ------------------------------------
        1.0        03.08.2019      gokhan.dogan       1. Created this package.
        2.0        06.05.2020      gokhan.dogan       1. GEO PART added.
        2.1        09.05.2020      gokhan.dogan       1. implemented new rollback process  
        2.3        16.06.2020      gokhan.dogan       1. implemented PISON-11617,PISON-13496,PISON-13509
        2.4        17.06.2020      gokhan.dogan       1. PBR_UTL_CONSTARINTS For weak covearge, rollback kpi cluster generation sclid=nclid 
        2.5        23.06.2020      gokhan.dogan       1. implemented some fixes.
                                                                    
   ******************************************************************************/
     
   /********* Global parameters definations ************/
   TYPE T_GENERAL_VARIABLES IS RECORD 
   (
     EXECUTIONGUID     RAW(16),
     EXECUTIONSTARTTIMESTAMP  TIMESTAMP,
     EXECUTIONPLANID NUMBER,
     OPTIMIZER_NAME VARCHAR2(200),
     SCHEDULER_PERIOD VARCHAR2(50),
     IS_PERIOD_ROLLBACK NUMBER,
     EXECUTIONPROFILE XMLTYPE,
     EXECUTIONPARAMS XMLTYPE,
     OBSERVATIONPERIODS XMLTYPE,
     OPERATION_TYPE NUMBER,
     SESSION_ID NUMBER,
     TAB_OBJECT_ID1 NUMBER,
     ROP_START_DATE DATE,
     ROP_END_DATE DATE,
     WEAK_COVERAGE_SWITCH VARCHAR2(20),
     BAD_QUALITY_SWITCH VARCHAR2(20)
   );
     v_TA90ExtensionForGapDetection CONSTANT NUMBER := 1.3; /* PISON-8509 */
     v_MinExpectedBeamwidth         CONSTANT NUMBER := 65 ;/* PISON-8490 */
     v_MaximumInsideArcDistance     CONSTANT NUMBER   := 20;
     V_ROW_LS_CCO_SETTINGS T_GENERAL_VARIABLES;  
     v_test number;
     v_workingBinIndex number:=1;

  PROCEDURE RAISE_EXCEPTION(PMESSAGE VARCHAR2,PLINE_NUMBER NUMBER)
   IS 
   BEGIN
    Raise_Application_Error (-20001,PMESSAGE || ' Line Number:' || PLINE_NUMBER );
   END; 
  
procedure TRUNCATE_TEMP_TABLES
is begin 
 --- ON COMMIT PRESERVE ROWS
execute immediate 'TRUNCATE TABLE LS_CCO_MT_TEMP_CLUSTER_KPIS';
execute immediate 'TRUNCATE TABLE LS_CCO_TEMP_HISTOGRAMS';
execute immediate 'TRUNCATE TABLE LS_CCO_MT_T_INSIDECLUSTERBINS';
--execute immediate 'TRUNCATE TABLE LS_CCO_MT_ALL_RELS';
end;
  
 PROCEDURE FILL_SETTINGS_TABLE
   IS
   BEGIN   
      
       /****************** FILLING 4G SETTINGS **************************************************/
   INSERT INTO LS_CCO_MT_GENERAL_SETTINGS (EXECUTIONGUID,EXECUTIONSTARTTIMESTAMP,PROFILEID,PROFILE_NAME, TECH_TYPE,CLID,CCO_ACTIVE,CCO_AVAIL,CCO_CLUSTER_SWITCH,
                                            CLUSTERSIZECELLCOUNT,
              OVERSHOOT_SWITCH, OVERSHOOT_ALLOWED_PERC, OVERSHOOT_MIN_ACTIVE_NE, OVERSHOOT_MIN_SITE_IN_AREA, UNDERSHOOTER_SWITCH, UNDERSHOOT_IRAT_PER_CALL, 
            UNDERSHOT_IRAT_ATTEMPT,UNDERSHOOT_BORDER_TRAFFIC_RT, UNDERSHOOT_MAX_ACTIVE_NE, UNDERSHOOT_TOLERATED_ERAB_DIF, UNDERSHOOT_ALLOWED_HO_PERCALL, UNDERSHOOT_CRITICAL_BAD_COV, 
            TILT_SWITCH, UNDERSOOT_ALLOWED_PRB_UTIL,
            TILT_DELTA_MIN_UI, TILT_DELTA_MAX_UI, TILT_MIN_UI ,TILT_MAX_UI, 
            CARRIERPOWER_SWITCH, 
            POWER_DELTA_MIN_UI, POWER_DELTA_MAX_UI,  POWER_MIN_UI, POWER_MAX_UI, 
            REPEAT_AVOIDANCE_SWITCH, AVOIDANCE_PERIOD, COVERAGE_PROTECTION_MULTIPLIER, MAXHEIGHTTHD, RELATIVEAZIMUTHTHD, MAXIMUM_RESOURCE_UTILIZATION, MAXIMUM_CAPACITY_FAILURE, 
            ROLLBACK_SWITCH, ROLLBACK_EXCLUDED_SWITCH, ROLLBACK_RESOURCE_UTIL, ROLLBACK_MAX_CAPACITY_FAILURE, ROLLBACK_VOICE_DROP_RATE, ROLLBACK_NUMBER_OF_VOICE_DROP, 
            ROLLBACK_PACKET_DROP_RATE, ROLLBACK_NUMBER_OF_PACKET_DROP, ROLLBACK_IRAT_HO_ACTIVIT_CALL, ROLLBACK_NUMBER_IRAT_HO_ACT, ROLLBACK_CA_DATAVOLUME, 
            ROLLBACK_CLUSTER_DATA_VOLUME, ROLLBACK_CLUSTER_VOICE_TRAFFIC,
            ROP_STARTTIME,ROP_ENDTIME,
            WEAK_COVERAGE_SWITCH, WEAK_CELL_THRESHOLD, WEAK_RSRP_TRESHOLD, WEAK_TILT_COEFF, WEAK_HEIGHT_COEFF, WEAK_DISTANCE_COEFF, 
            WEAK_RELATIVE_AZIMUTH_COEFF, WEAK_PRBUTILIZATION_COEFF, WEAK_COVERAGE_COEFF,
            BAD_QUALITY_SWITCH ,BAD_RSRP_THRESHOLD ,BAD_RSRQ_THRESHOLD, BAD_QUALITY_PERCENTE  , BAD_POLLUTER_RSRP_DIFF, BAD_MIN_NUMBEROFPOLLUTER
            )        
  SELECT    V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID,
            V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,
            PROFILEID,PROFILENAME, '4G' AS TECH_TYPE, 322 AS CLID, CCO_4G_ACTIVE,CCO_AVAIL,CCO_CREATEISOLATEDCLUSTER,CLUSTERSIZECELLCOUNT, 
            OVERSHOOT_SWITCH, 
            OVERSHOOT_ALLOWED_PERC /100 /* make it percentage */,
            OVERSHOOT_MIN_ACTIVE_NE, OVERSHOOT_MIN_SITE_IN_AREA, UNDERSHOOTER_SWITCH, UNDERSHOOT_IRAT_PER_CALL, 
            UNDERSHOT_IRAT_ATTEMPT,UNDERSHOOT_BORDER_TRAFFIC_RT, UNDERSHOOT_MAX_ACTIVE_NE, UNDERSHOOT_TOLERATED_ERAB_DIF, UNDERSHOOT_ALLOWED_HO_PERCALL, UNDERSHOOT_CRITICAL_BAD_COV, 
            ETILT_SWITCH, UNDERSOOT_ALLOWED_PRB_UTIL, 
            TILT_DELTA_MIN_UI, TILT_DELTA_MAX_UI, TILT_MIN_UI, TILT_MAX_UI, 
            CARRIERPOWER_SWITCH, 
            POWER_DELTA_MIN_UI, POWER_DELTA_MAX_UI,  POWER_MIN_UI, POWER_MAX_UI, 
            REPEAT_AVOIDANCE_SWITCH, AVOIDANCE_PERIOD, COVERAGEPROTECTIONMULTIPLIER, MAXHEIGHTTHD, RELATIVEAZIMUTHTHD, MAXIMUMRESOURCEUTILIZATION, MAXIMUMCAPACITYFAILURE, 
            ROLLBACK_SWITCH, ROLLBACK_EXCLUDED_SWITCH, ROLLBACK_RESOURCE_UTIL, ROLLBACK_MAX_CAPACITY_FAULE, ROLLBACK_VOICE_DROP_RATE, ROLLBACK_NUMBER_OF_VOICE_DROP, 
            ROLLBACK_PACKET_DROP_RATE, ROLLBACK_NUMBER_OF_PACKET_DROP, ROLLBACK_IRAT_HO_ACTIVIT_CALL, ROLLBACK_NUMBER_IRAT_HO_ACT, 
            - ROLLBACK_CA_DATAVOLUME, -- negative sign
            - ROLLBACK_CLUSTER_DATA_VOLUME, -- negative sign
            - ROLLBACK_CLUSTER_VOICE_TRAFFIC, -- negative sign
            LITESON_HELPERS.GET_STARTDATE(V_ROW_LS_CCO_SETTINGS.OBSERVATIONPERIODS,'ActionPeriod',PR.PROFILEID),
            LITESON_HELPERS.GET_ENDDATE(V_ROW_LS_CCO_SETTINGS.OBSERVATIONPERIODS,'ActionPeriod',PR.PROFILEID),
             WEAK_COVERAGE_SWITCH, WEAK_CELLTHRESHOLD /100, WEAK_RSRP_TRESHOLD, 
             WEAK_TILTCOEFF,  
             WEAK_HEIGHTCOEFF  , 
             WEAK_DISTANCECOEFF  , 
             WEAK_RELATIVEAZIMUTHCOEFF  , WEAK_PRBUTILIZATIONCOEFFICIENT  , WEAK_COVERAGECOEFFICIENT,
            BAD_QUALITY_SWITCH ,BAD_RSRP_THRESHOLD ,BAD_RSRQ_THRESHOLD, BAD_QUALITY_PERCENTE / 100 , BAD_POLLUTER_RSRP_DIFF, BAD_MIN_NUMBEROFPOLLUTER
        FROM XMLTABLE ( '/ExecutionPlan/ExecutionPlanProfiles/ExecutionPlanProfile'
                            PASSING  V_ROW_LS_CCO_SETTINGS.EXECUTIONPROFILE
                            COLUMNS 
                            ProfileId NUMBER (12) PATH 'Profile/Id',
                            ProfileName VARCHAR2(100 BYTE) PATH 'Profile/Name',
                            ProfileType VARCHAR2 (30 BYTE) PATH 'Profile/Type',
                             XmlGroups XMLTYPE  PATH 'ProfileParameters/GenericProfile/Groups'
                       ) pr
            CROSS JOIN
      XMLTABLE 
      (
       '/Groups'  
      PASSING pr.XmlGroups COLUMNS
      CCO_4G_ACTIVE                 VARCHAR2 (32 BYTE) PATH 'Group[GroupName="GeneralSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ObservationPeriods"]/Fields/Field[Name="CCO_4G_ACTIVE"]/Value',
      CCO_Avail                      VARCHAR2 (32 BYTE) PATH 'Group[GroupName="GeneralSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ObservationPeriods"]/Fields/Field[Name="CCO_Avail"]/Value',
      CCO_CreateIsolatedCluster      VARCHAR2 (32 BYTE) PATH 'Group[GroupName="GeneralSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ObservationPeriods"]/Fields/Field[Name="CCO_CreateIsolatedCluster"]/Value',
      ClusterSizeCellCount           number PATH 'Group[GroupName="GeneralSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ObservationPeriods"]/Fields/Field[Name="ClusterSizeCellCount"]/Value',
      CAT1_ARFCN_LIST  XMLTYPE PATH  'Group[GroupName="GeneralSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="CoverageLayerSettings"]/Fields/Field[Name="CAT1_ARFCN_LIST"]/Values',
      CAT2_ARFCN_LIST  XMLTYPE PATH  'Group[GroupName="GeneralSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="CoverageLayerSettings"]/Fields/Field[Name="CAT2_ARFCN_LIST"]/Values',
      CAT3_ARFCN_LIST  XMLTYPE PATH  'Group[GroupName="GeneralSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="CoverageLayerSettings"]/Fields/Field[Name="CAT3_ARFCN_LIST"]/Values',
      CAT4_ARFCN_LIST  XMLTYPE PATH  'Group[GroupName="GeneralSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="CoverageLayerSettings"]/Fields/Field[Name="CAT4_ARFCN_LIST"]/Values',
      POLICY_MAPPING   XMLTYPE PATH  'Group[GroupName="GeneralSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ActionMappingPolicy"]/Fields/Field/TableRows',
      OVERSHOOT_SWITCH               VARCHAR2 (32 BYTE) PATH 'Group[GroupName="OvershooterCellDetectionandResolution4G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="OvershooterCellDetectionandResolution"]/Fields/Field[Name="CCO_OVERSHOOT_ACTIVE"]/Value',
      OVERSHOOT_ALLOWED_PERC         VARCHAR2 (32 BYTE) PATH 'Group[GroupName="OvershooterCellDetectionandResolution4G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="OvershooterCellDetectionandResolution"]/Fields/Field[Name="AllowedOvershootPercentage_4G"]/Value',
      OVERSHOOT_MIN_ACTIVE_NE        VARCHAR2 (32 BYTE) PATH 'Group[GroupName="OvershooterCellDetectionandResolution4G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="OvershooterCellDetectionandResolution"]/Fields/Field[Name="MinimumNumberofActiveNeighbors"]/Value',
      OVERSHOOT_MIN_SITE_IN_AREA     VARCHAR2 (32 BYTE) PATH 'Group[GroupName="OvershooterCellDetectionandResolution4G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="OvershooterCellDetectionandResolution"]/Fields/Field[Name="MinNofSitesInServingArea"]/Value',
      UNDERSHOOTER_SWITCH            VARCHAR2 (32 BYTE) PATH 'Group[GroupName="UndershooterCellDetectionAndResolution4G"]/Tabs/Tab[Name="4GCoverage"]/Sections/Section[Name="UndershooterCellDetectionAndResolution"]/Fields/Field[Name="CCO_UNDERSHOOTER_ACTIVE"]/Value',
      UNDERSHOOT_IRAT_PER_CALL       VARCHAR2 (32 BYTE) PATH 'Group[GroupName="UndershooterCellDetectionAndResolution4G"]/Tabs/Tab[Name="4GCoverage"]/Sections/Section[Name="UndershooterCellDetectionAndResolution"]/Fields/Field[Name="AllowedIRATActivityperCallCG"]/Value',
      UNDERSHOT_IRAT_ATTEMPT         VARCHAR2 (32 BYTE) PATH 'Group[GroupName="UndershooterCellDetectionAndResolution4G"]/Tabs/Tab[Name="4GCoverage"]/Sections/Section[Name="UndershooterCellDetectionAndResolution"]/Fields/Field[Name="AllowedIRATAttempt"]/Value',
      UNDERSHOOT_BORDER_TRAFFIC_RT   NUMBER             PATH  'Group[GroupName="UndershooterCellDetectionAndResolution4G"]/Tabs/Tab[Name="4GCoverage"]/Sections/Section[Name="UndershooterCellDetectionAndResolution"]/Fields/Field[Name="AllowedBorderTrafficRatio"]/Value',
      UNDERSHOOT_MAX_ACTIVE_NE       VARCHAR2 (32 BYTE) PATH 'Group[GroupName="UndershooterCellDetectionAndResolution4G"]/Tabs/Tab[Name="4GCoverage"]/Sections/Section[Name="UndershooterCellDetectionAndResolution"]/Fields/Field[Name="MaximumNumberofActiveNeighborsCG"]/Value',
      UNDERSHOOT_TOLERATED_ERAB_DIF  VARCHAR2 (32 BYTE) PATH 'Group[GroupName="UndershooterCellDetectionAndResolution4G"]/Tabs/Tab[Name="4GCoverage"]/Sections/Section[Name="UndershooterCellDetectionAndResolution"]/Fields/Field[Name="ToleratedTrafficErabDifference"]/Value',
      UNDERSHOOT_ALLOWED_HO_PERCALL  VARCHAR2 (32 BYTE) PATH 'Group[GroupName="UndershooterCellDetectionAndResolution4G"]/Tabs/Tab[Name="4GCoverage"]/Sections/Section[Name="UndershooterCellDetectionAndResolution"]/Fields/Field[Name="Allow_Cov_HO_percall"]/Value',
      UNDERSHOOT_CRITICAL_BAD_COV    VARCHAR2 (32 BYTE) PATH 'Group[GroupName="UndershooterCellDetectionAndResolution4G"]/Tabs/Tab[Name="4GCoverage"]/Sections/Section[Name="UndershooterCellDetectionAndResolution"]/Fields/Field[Name="Critical_Bad_Cov_Threshold"]/Value',
      UNDERSOOT_ALLOWED_PRB_UTIL     VARCHAR2 (32 BYTE) PATH 'Group[GroupName="UndershooterCellDetectionAndResolution4G"]/Tabs/Tab[Name="4GCoverage"]/Sections/Section[Name="UndershooterCellDetectionAndResolution"]/Fields/Field[Name="AllowedMaxPRBUtilization"]/Value', 
      ETILT_SWITCH                   VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="electricalAntennaTilt"]/Value',
      TILT_DELTA_MIN_UI              VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="electricalAntennaTiltMinimumDeltaValue"]/Value',
      TILT_DELTA_MAX_UI              VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="electricalAntennaTiltMaximumDeltaValue"]/Value',
      TILT_MIN_UI                    VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="electricalAntennaTiltMinimumValue"]/Value',
      TILT_MAX_UI                    VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="electricalAntennaTiltMaximumValue"]/Value',
      CARRIERPOWER_SWITCH            VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="carrierPowerCG"]/Value',    
      POWER_DELTA_MIN_UI             VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="carrierPowerMinimumDeltaValue"]/Value',
      POWER_DELTA_MAX_UI             VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="carrierPowerMaximumDeltaValue"]/Value',
      POWER_MIN_UI                  VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="carrierPowerMinimumValue"]/Value',
      POWER_MAX_UI                   VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="carrierPowerMaximumValue"]/Value',
      REPEAT_AVOIDANCE_SWITCH        VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="Repeat_FW_BW_Avoidance"]/Value',
      Avoidance_Period               VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="Avoidance_Period"]/Value',   
      CoverageProtectionMultiplier   VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="UptiltActionConstraints"]/Fields/Field[Name="CoverageProtectionMultiplier"]/Value',
      MaxHeightThd                   VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="UptiltActionConstraints"]/Fields/Field[Name="MaxHeightThd"]/Value',
      RelativeAzimuthThd             VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="UptiltActionConstraints"]/Fields/Field[Name="RelativeAzimuthThd"]/Value',
      MaximumResourceUtilization     VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="UptiltActionConstraints"]/Fields/Field[Name="MaximumResourceUtilization"]/Value',
      MaximumCapacityFailure         VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="UptiltActionConstraints"]/Fields/Field[Name="MaximumCapacityFailure"]/Value',
      ROLLBACK_SWITCH                VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="Rollback"]/Fields/Field[Name="EnableRollback"]/Value',
      ROLLBACK_EXCLUDED_SWITCH       VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="Rollback"]/Fields/Field[Name="EnableRollbackIfExcluded"]/Value',
      ROLLBACK_RESOURCE_UTIL         VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="Rollback"]/Fields/Field[Name="MaximumResourceUtilizationRollback"]/Value',
      ROLLBACK_MAX_CAPACITY_FAULE    VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="Rollback"]/Fields/Field[Name="MaximumCapacityFailuresRollback"]/Value',
      ROLLBACK_VOICE_DROP_RATE       VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="PercentageDeltaThresholdsforRollback"]/Fields/Field[Name="VoiceDropRateThreshold"]/Value',
      ROLLBACK_NUMBER_OF_VOICE_DROP  VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="PercentageDeltaThresholdsforRollback"]/Fields/Field[Name="NumberofVoiceDrops"]/Value',
      ROLLBACK_PACKET_DROP_RATE      VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="PercentageDeltaThresholdsforRollback"]/Fields/Field[Name="PacketDropRateThreshold"]/Value',
      ROLLBACK_NUMBER_OF_PACKET_DROP VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="PercentageDeltaThresholdsforRollback"]/Fields/Field[Name="NumberofPacketDrops"]/Value',
      ROLLBACK_IRAT_HO_ACTIVIT_CALL  VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="PercentageDeltaThresholdsforRollback"]/Fields/Field[Name="IRATHandoverActivityperCall"]/Value',
      ROLLBACK_NUMBER_IRAT_HO_ACT    VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="PercentageDeltaThresholdsforRollback"]/Fields/Field[Name="NumberofIRATHandoverActivity"]/Value',
      ROLLBACK_CA_DATAVOLUME         VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="PercentageDeltaThresholdsforRollback"]/Fields/Field[Name="CADataVolume"]/Value',
      ROLLBACK_CLUSTER_DATA_VOLUME   VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="PercentageDeltaThresholdsforRollback"]/Fields/Field[Name="ClusterDataVolume"]/Value',
      ROLLBACK_Cluster_Voice_Traffic VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="PercentageDeltaThresholdsforRollback"]/Fields/Field[Name="ClusterVoiceTraffic"]/Value',
      WEAK_COVERAGE_SWITCH           VARCHAR2 (32 BYTE) PATH 'Group[GroupName="WeakCoverageDetectionandResolution4G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="WeakCoverageDetectionandResolution"]/Fields/Field[Name="CCO_WEAKCOVERAGE_ACTIVE"]/Value',
      WEAK_CELLTHRESHOLD             VARCHAR2 (32 BYTE) PATH 'Group[GroupName="WeakCoverageDetectionandResolution4G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="WeakCoverageDetectionandResolution"]/Fields/Field[Name="WorstCellThreshold"]/Value',
      WEAK_RSRP_TRESHOLD             VARCHAR2 (32 BYTE) PATH 'Group[GroupName="WeakCoverageDetectionandResolution4G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="WeakCoverageDetectionandResolution"]/Fields/Field[Name="BadCoverageRrspThreshold"]/Value',
      WEAK_TILTCOEFF                 VARCHAR2 (32 BYTE) PATH 'Group[GroupName="WeakCoverageDetectionandResolution4G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="Weak Coverage Coefficients"]/Fields/Field[Name="tiltCoeff"]/Value',
      WEAK_HEIGHTCOEFF               VARCHAR2 (32 BYTE) PATH 'Group[GroupName="WeakCoverageDetectionandResolution4G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="Weak Coverage Coefficients"]/Fields/Field[Name="heightCoeff"]/Value',
      WEAK_DISTANCECOEFF             VARCHAR2 (32 BYTE) PATH 'Group[GroupName="WeakCoverageDetectionandResolution4G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="Weak Coverage Coefficients"]/Fields/Field[Name="distanceCoeff"]/Value',
      WEAK_RELATIVEAZIMUTHCOEFF      VARCHAR2 (32 BYTE) PATH 'Group[GroupName="WeakCoverageDetectionandResolution4G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="Weak Coverage Coefficients"]/Fields/Field[Name="relativeAzimuthCoeff"]/Value',
      WEAK_PRBUTILIZATIONCOEFFICIENT VARCHAR2 (32 BYTE) PATH 'Group[GroupName="WeakCoverageDetectionandResolution4G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="Weak Coverage Coefficients"]/Fields/Field[Name="PrbUtilizationCoefficient"]/Value' ,  
      WEAK_COVERAGECOEFFICIENT       VARCHAR2 (32 BYTE) PATH 'Group[GroupName="WeakCoverageDetectionandResolution4G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="Weak Coverage Coefficients"]/Fields/Field[Name="WeakCoverageCoefficient"]/Value'  ,
      BAD_QUALITY_SWITCH       VARCHAR2 (32 BYTE) PATH 'Group[GroupName="BadQualityDetectionandResolution4G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="BadQualityDetectionAndResolution"]/Fields/Field[Name="Cco_BadQuality_Active"]/Value',
      BAD_RSRP_THRESHOLD       VARCHAR2 (32 BYTE) PATH 'Group[GroupName="BadQualityDetectionandResolution4G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="BadQualityDetectionAndResolution"]/Fields/Field[Name="RsrpBadQualityThresholddBm"]/Value',
      BAD_RSRQ_THRESHOLD       VARCHAR2 (32 BYTE) PATH 'Group[GroupName="BadQualityDetectionandResolution4G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="BadQualityDetectionAndResolution"]/Fields/Field[Name="RsrqBadQualityThresholddB"]/Value',
      BAD_QUALITY_PERCENTE     VARCHAR2 (32 BYTE) PATH 'Group[GroupName="BadQualityDetectionandResolution4G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="BadQualityDetectionAndResolution"]/Fields/Field[Name="WorstBadQualityPercentile"]/Value',
      BAD_POLLUTER_RSRP_DIFF   VARCHAR2 (32 BYTE) PATH 'Group[GroupName="BadQualityDetectionandResolution4G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="BadQualityDetectionAndResolution"]/Fields/Field[Name="PolluterMaxRSRPDiff"]/Value',
      BAD_MIN_NUMBEROFPOLLUTER VARCHAR2 (32 BYTE) PATH 'Group[GroupName="BadQualityDetectionandResolution4G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="BadQualityDetectionAndResolution"]/Fields/Field[Name="MinNumberOfPolluter"]/Value' 
   ) m1
   where CCO_4G_ACTIVE='true';
    
   /********************** FILLING 3G SETTINGS  *******************************************************/
     
INSERT INTO LS_CCO_MT_GENERAL_SETTINGS (EXECUTIONGUID,EXECUTIONSTARTTIMESTAMP,PROFILEID,PROFILE_NAME,TECH_TYPE,CLID,CCO_ACTIVE, CCO_AVAIL, CCO_CLUSTER_SWITCH, 
                CLUSTERSIZECELLCOUNT, 
                OVERSHOOT_SWITCH, OVERSHOOT_ALLOWED_PERC, OVERSHOOT_SHO_OVERHEAD, OVERSHOOT_MIN_ACTIVE_NE, OVERSHOOT_MIN_SITE_IN_AREA, UNDERSHOOTER_SWITCH, 
            UNDERSHOOT_SHO_OVERHEAD, UNDERSHOOT_IRAT_PER_CALL, UNDERSHOT_IRAT_ATTEMPT, UNDERSHOOT_MAX_ACTIVE_NE,UNDERSHOOT_TOLERATED_TRAFF, TILT_SWITCH,   
            TILT_DELTA_MIN_UI, TILT_DELTA_MAX_UI, TILT_MIN_UI, TILT_MAX_UI, 
            CARRIERPOWER_SWITCH, 
            POWER_DELTA_MIN_UI, POWER_DELTA_MAX_UI,  POWER_MIN_UI, POWER_MAX_UI, 
             REPEAT_AVOIDANCE_SWITCH,
            AVOIDANCE_PERIOD, COVERAGE_PROTECTION_MULTIPLIER, MAXIMUM_RESOURCE_UTILIZATION, MAXIMUM_CAPACITY_FAILURE, ROLLBACK_SWITCH, ROLLBACK_EXCLUDED_SWITCH, 
            ROLLBACK_RESOURCE_UTIL, ROLLBACK_MAX_CAPACITY_FAILURE, ROLLBACK_VOICE_DROP_RATE, ROLLBACK_NUMBER_OF_VOICE_DROP, ROLLBACK_PACKET_DROP_RATE, 
            ROLLBACK_NUMBER_OF_PACKET_DROP, ROLLBACK_IRAT_HO_ACTIVIT_CALL, ROLLBACK_NUMBER_IRAT_HO_ACT, ROLLBACK_CA_DATAVOLUME, ROLLBACK_CLUSTER_DATA_VOLUME, 
            ROLLBACK_CLUSTER_VOICE_TRAFFIC ,
            ROP_STARTTIME,ROP_ENDTIME,
            WEAK_COVERAGE_SWITCH, WEAK_CELL_THRESHOLD, WEAK_RSCP_THRESHOLD, WEAK_TILT_COEFF, WEAK_HEIGHT_COEFF, WEAK_DISTANCE_COEFF,WEAK_PRBUTILIZATION_COEFF, WEAK_COVERAGE_COEFF,
            BAD_QUALITY_SWITCH, BAD_RSCP_THRESHOLD, BAD_ECNO_THRESHOLD, BAD_QUALITY_PERCENTE , BAD_MIN_THRESHOLD) 
  SELECT  
            V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID,
            V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,
            PROFILEID,ProfileName,'3G' AS TECH_TYPE,321 AS CLID, CCO_3G_ACTIVE, CCO_AVAIL, CCO_CREATEISOLATEDCLUSTER, CLUSTERSIZECELLCOUNT,    
            OVERSHOOT_SWITCH, 
            OVERSHOOT_ALLOWED_PERC /100 /* make it percentage */,
             OVERSHOOT_SHO_OVERHEAD, OVERSHOOT_MIN_ACTIVE_NE, 
            OVERSHOOT_MIN_SITE_IN_AREA, UNDERSHOOTER_SWITCH,UNDERSHOOT_SHO_OVERHEAD, UNDERSHOOT_IRAT_PER_CALL, UNDERSHOT_IRAT_ATTEMPT, UNDERSHOOT_MAX_ACTIVE_NE, 
            UNDERSHOOT_TOLERATED_TRAFF /100 ,
            ETILT_SWITCH,
            TILT_DELTA_MIN_UI, TILT_DELTA_MAX_UI, TILT_MIN_UI, TILT_MAX_UI, 
            CARRIERPOWER_SWITCH, 
            POWER_DELTA_MIN_UI, POWER_DELTA_MAX_UI,  POWER_MIN_UI, POWER_MAX_UI, 
            REPEAT_AVOIDANCE_SWITCH,AVOIDANCE_PERIOD, COVERAGEPROTECTIONMULTIPLIER, MAXIMUMRESOURCEUTILIZATION, 
            MAXIMUMCAPACITYFAILURE, ROLLBACK_SWITCH, ROLLBACK_EXCLUDED_SWITCH,ROLLBACK_RESOURCE_UTIL, ROLLBACK_MAX_CAPACITY_FAULE, ROLLBACK_VOICE_DROP_RATE, 
            ROLLBACK_NUMBER_OF_VOICE_DROP, ROLLBACK_PACKET_DROP_RATE,ROLLBACK_NUMBER_OF_PACKET_DROP, ROLLBACK_IRAT_HO_ACTIVIT_CALL, ROLLBACK_NUMBER_IRAT_HO_ACT,            
            - ROLLBACK_CA_DATAVOLUME, -- negative sign
            - ROLLBACK_CLUSTER_DATA_VOLUME, -- negative sign
            - ROLLBACK_CLUSTER_VOICE_TRAFFIC, -- negative sign
            LITESON_HELPERS.GET_STARTDATE(V_ROW_LS_CCO_SETTINGS.OBSERVATIONPERIODS,'ActionPeriod',PR.PROFILEID),
            LITESON_HELPERS.GET_ENDDATE(V_ROW_LS_CCO_SETTINGS.OBSERVATIONPERIODS,'ActionPeriod',PR.PROFILEID),
             WEAK_COVERAGE_SWITCH, 
             WEAK_CELL_THRESHOLD /100 , WEAK_RSCP_THRESHOLD /100 , WEAK_TILT_COEFF /100 , WEAK_HEIGHT_COEFF /100 , WEAK_DISTANCE_COEFF /100,
             WEAK_PRBUTILIZATION_COEFF /100 , WEAK_COVERAGE_COEFF /100 ,
            BAD_QUALITY_SWITCH, BAD_RSCP_THRESHOLD, BAD_ECNO_THRESHOLD, BAD_QUALITY_PERCENTE /100 , BAD_MIN_THRESHOLD  /100  
        FROM XMLTABLE ( '/ExecutionPlan/ExecutionPlanProfiles/ExecutionPlanProfile'
                            PASSING  V_ROW_LS_CCO_SETTINGS.EXECUTIONPROFILE
                            COLUMNS 
                            ProfileId NUMBER (12) PATH 'Profile/Id',
                            ProfileName VARCHAR2(100 BYTE) PATH 'Profile/Name',
                            ProfileType VARCHAR2 (30 BYTE) PATH 'Profile/Type',
                             XmlGroups XMLTYPE  PATH 'ProfileParameters/GenericProfile/Groups'
                       ) pr
            CROSS JOIN
      XMLTABLE 
      (
       '/Groups'
      PASSING pr.XmlGroups COLUMNS
      CCO_3G_ACTIVE               VARCHAR2 (32 BYTE) PATH 'Group[GroupName="GeneralSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ObservationPeriods"]/Fields/Field[Name="CCO_3G_ACTIVE"]/Value',
      CCO_Avail                   VARCHAR2 (32 BYTE) PATH 'Group[GroupName="GeneralSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ObservationPeriods"]/Fields/Field[Name="CCO_Avail"]/Value',
      CCO_CreateIsolatedCluster   VARCHAR2 (32 BYTE) PATH 'Group[GroupName="GeneralSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ObservationPeriods"]/Fields/Field[Name="CCO_CreateIsolatedCluster"]/Value',
      ClusterSizeCellCount        NUMBER PATH 'Group[GroupName="GeneralSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ObservationPeriods"]/Fields/Field[Name="ClusterSizeCellCount"]/Value',
      OVERSHOOT_SWITCH            VARCHAR2 (32 BYTE) PATH 'Group[GroupName="OvershooterCellDetectionandResolution3G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="OvershooterCellDetectionandResolution"]/Fields/Field[Name="CCO_OVERSHOOT_ACTIVE"]/Value',
      OVERSHOOT_ALLOWED_PERC      VARCHAR2 (32 BYTE) PATH 'Group[GroupName="OvershooterCellDetectionandResolution3G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="OvershooterCellDetectionandResolution"]/Fields/Field[Name="AllowedOvershootPercentage_3G"]/Value',
      OVERSHOOT_SHO_OVERHEAD      VARCHAR2 (32 BYTE) PATH 'Group[GroupName="OvershooterCellDetectionandResolution3G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="OvershooterCellDetectionandResolution"]/Fields/Field[Name="CCO_OVERSHOOT_SHO_OVERHEAD"]/Value',
      OVERSHOOT_MIN_ACTIVE_NE     VARCHAR2 (32 BYTE) PATH 'Group[GroupName="OvershooterCellDetectionandResolution3G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="OvershooterCellDetectionandResolution"]/Fields/Field[Name="MinimumNumberofActiveNeighbors"]/Value',
      OVERSHOOT_MIN_SITE_IN_AREA  VARCHAR2 (32 BYTE) PATH 'Group[GroupName="OvershooterCellDetectionandResolution3G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="OvershooterCellDetectionandResolution"]/Fields/Field[Name="MinNofSitesInServingArea"]/Value',
      UNDERSHOOTER_SWITCH         VARCHAR2 (32 BYTE) PATH 'Group[GroupName="UndershooterCellDetectionAndResolution3G"]/Tabs/Tab[Name="3GCoverage"]/Sections/Section[Name="UndershooterCellDetectionAndResolution"]/Fields/Field[Name="CCO_UNDERSHOOTER_ACTIVE"]/Value',
      UNDERSHOOT_SHO_OVERHEAD     VARCHAR2 (32 BYTE) PATH 'Group[GroupName="UndershooterCellDetectionAndResolution3G"]/Tabs/Tab[Name="3GCoverage"]/Sections/Section[Name="UndershooterCellDetectionAndResolution"]/Fields/Field[Name="CG_UNDERSHOOT_SHO_OVERHEAD"]/Value',
      UNDERSHOOT_IRAT_PER_CALL    VARCHAR2 (32 BYTE) PATH 'Group[GroupName="UndershooterCellDetectionAndResolution3G"]/Tabs/Tab[Name="3GCoverage"]/Sections/Section[Name="UndershooterCellDetectionAndResolution"]/Fields/Field[Name="AllowedIRATActivityperCallCG"]/Value',
      UNDERSHOT_IRAT_ATTEMPT      VARCHAR2 (32 BYTE) PATH 'Group[GroupName="UndershooterCellDetectionAndResolution3G"]/Tabs/Tab[Name="3GCoverage"]/Sections/Section[Name="UndershooterCellDetectionAndResolution"]/Fields/Field[Name="AllowedIRATAttempt"]/Value',
      UNDERSHOOT_MAX_ACTIVE_NE    VARCHAR2 (32 BYTE) PATH 'Group[GroupName="UndershooterCellDetectionAndResolution3G"]/Tabs/Tab[Name="3GCoverage"]/Sections/Section[Name="UndershooterCellDetectionAndResolution"]/Fields/Field[Name="MaximumNumberofActiveNeighborsCG"]/Value',
      UNDERSHOOT_TOLERATED_TRAFF  VARCHAR2 (32 BYTE) PATH 'Group[GroupName="UndershooterCellDetectionAndResolution3G"]/Tabs/Tab[Name="3GCoverage"]/Sections/Section[Name="UndershooterCellDetectionAndResolution"]/Fields/Field[Name="ToleratedTrafficDifference"]/Value',    
      ETILT_SWITCH                   VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="electricalAntennaTilt"]/Value',
      TILT_DELTA_MIN_UI              VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="electricalAntennaTiltMinimumDeltaValue"]/Value',
      TILT_DELTA_MAX_UI              VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="electricalAntennaTiltMaximumDeltaValue"]/Value',
      TILT_MIN_UI                    VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="electricalAntennaTiltMinimumValue"]/Value',
      TILT_MAX_UI                    VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="electricalAntennaTiltMaximumValue"]/Value',
      CARRIERPOWER_SWITCH            VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="carrierPowerCG"]/Value',
      POWER_DELTA_MIN_UI             VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="carrierPowerMinimumDeltaValue"]/Value',
      POWER_DELTA_MAX_UI             VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="carrierPowerMaximumDeltaValue"]/Value',
      POWER_MIN_UI                   VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="carrierPowerMinimumValue"]/Value',
      POWER_MAX_UI                   VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="carrierPowerMaximumValue"]/Value',
      REPEAT_AVOIDANCE_SWITCH        VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="Repeat_FW_BW_Avoidance"]/Value',
      Avoidance_Period               VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ManagedParameters"]/Fields/Field[Name="Avoidance_Period"]/Value',   
      CoverageProtectionMultiplier   VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="UptiltActionConstraints"]/Fields/Field[Name="CoverageProtectionMultiplier"]/Value',
      MaximumResourceUtilization     VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="UptiltActionConstraints"]/Fields/Field[Name="MaximumResourceUtilization"]/Value',
      MaximumCapacityFailure         VARCHAR2 (32 BYTE) PATH 'Group[GroupName="ManagedParameters"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="UptiltActionConstraints"]/Fields/Field[Name="MaximumCapacityFailure"]/Value',
      ROLLBACK_SWITCH                VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="Rollback"]/Fields/Field[Name="EnableRollback"]/Value',
      ROLLBACK_EXCLUDED_SWITCH       VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="Rollback"]/Fields/Field[Name="EnableRollbackIfExcluded"]/Value',
      ROLLBACK_RESOURCE_UTIL         VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="Rollback"]/Fields/Field[Name="MaximumResourceUtilizationRollback"]/Value',
      ROLLBACK_MAX_CAPACITY_FAULE    VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="Rollback"]/Fields/Field[Name="MaximumCapacityFailuresRollback"]/Value',
      ROLLBACK_VOICE_DROP_RATE       VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="PercentageDeltaThresholdsforRollback"]/Fields/Field[Name="VoiceDropRateThreshold"]/Value',
      ROLLBACK_NUMBER_OF_VOICE_DROP  VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="PercentageDeltaThresholdsforRollback"]/Fields/Field[Name="NumberofVoiceDrops"]/Value',
      ROLLBACK_PACKET_DROP_RATE      VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="PercentageDeltaThresholdsforRollback"]/Fields/Field[Name="PacketDropRateThreshold"]/Value',
      ROLLBACK_NUMBER_OF_PACKET_DROP VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="PercentageDeltaThresholdsforRollback"]/Fields/Field[Name="NumberofPacketDrops"]/Value',
      ROLLBACK_IRAT_HO_ACTIVIT_CALL  VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="PercentageDeltaThresholdsforRollback"]/Fields/Field[Name="IRATHandoverActivityperCall"]/Value',
      ROLLBACK_NUMBER_IRAT_HO_ACT    VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="PercentageDeltaThresholdsforRollback"]/Fields/Field[Name="NumberofIRATHandoverActivity"]/Value',
      ROLLBACK_CA_DATAVOLUME         VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="PercentageDeltaThresholdsforRollback"]/Fields/Field[Name="CADataVolume"]/Value',
      ROLLBACK_CLUSTER_DATA_VOLUME   VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="PercentageDeltaThresholdsforRollback"]/Fields/Field[Name="ClusterDataVolume"]/Value',
      ROLLBACK_Cluster_Voice_Traffic  VARCHAR2 (32 BYTE) PATH 'Group[GroupName="RollbackSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="PercentageDeltaThresholdsforRollback"]/Fields/Field[Name="ClusterVoiceTraffic"]/Value',
      WEAK_COVERAGE_SWITCH            VARCHAR2 (32 BYTE) PATH 'Group[GroupName="WeakCoverageDetectionandResolution3G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="WeakCoverageDetectionandResolution"]/Fields/Field[Name="CCO_WEAKCOVERAGE_ACTIVE"]/Value',
      WEAK_CELL_THRESHOLD             number PATH 'Group[GroupName="WeakCoverageDetectionandResolution3G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="WeakCoverageDetectionandResolution"]/Fields/Field[Name="WorstCellThreshold"]/Value',
      WEAK_RSCP_THRESHOLD             number PATH 'Group[GroupName="WeakCoverageDetectionandResolution3G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="WeakCoverageDetectionandResolution"]/Fields/Field[Name="WeakCoverageRscpThreshold"]/Value',
      WEAK_TILT_COEFF                 number PATH 'Group[GroupName="WeakCoverageDetectionandResolution3G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="Weak Coverage Coefficients"]/Fields/Field[Name="tiltCoeff"]/Value',
      WEAK_HEIGHT_COEFF               number PATH 'Group[GroupName="WeakCoverageDetectionandResolution3G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="Weak Coverage Coefficients"]/Fields/Field[Name="heightCoeff"]/Value',
      WEAK_DISTANCE_COEFF             number PATH 'Group[GroupName="WeakCoverageDetectionandResolution3G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="Weak Coverage Coefficients"]/Fields/Field[Name="distanceCoeff"]/Value',
      WEAK_PRBUTILIZATION_COEFF       number PATH 'Group[GroupName="WeakCoverageDetectionandResolution3G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="Weak Coverage Coefficients"]/Fields/Field[Name="PrbUtilizationCoefficient"]/Value' ,  
      WEAK_COVERAGE_COEFF             number PATH 'Group[GroupName="WeakCoverageDetectionandResolution3G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="Weak Coverage Coefficients"]/Fields/Field[Name="WeakCoverageCoefficient"]/Value' ,
      BAD_QUALITY_SWITCH     VARCHAR2 (32 BYTE) PATH 'Group[GroupName="BadQualityDetectionandResolution3G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="BadQualityDetectionAndResolution"]/Fields/Field[Name="Cco_BadQuality_Active"]/Value',
      BAD_RSCP_THRESHOLD    number PATH 'Group[GroupName="BadQualityDetectionandResolution3G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="BadQualityDetectionAndResolution"]/Fields/Field[Name="RscpBadQualityThresholddBm"]/Value',
      BAD_ECNO_THRESHOLD    number PATH 'Group[GroupName="BadQualityDetectionandResolution3G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="BadQualityDetectionAndResolution"]/Fields/Field[Name="EcNoBadQualityThresholddB"]/Value',
      BAD_QUALITY_PERCENTE  number PATH 'Group[GroupName="BadQualityDetectionandResolution3G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="BadQualityDetectionAndResolution"]/Fields/Field[Name="WorstBadQualityPercentile"]/Value',
      BAD_MIN_THRESHOLD     number PATH 'Group[GroupName="BadQualityDetectionandResolution3G"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="BadQualityDetectionAndResolution"]/Fields/Field[Name="Measurement_Threshold_BadQual"]/Value' 
  ) m1
  where CCO_3G_ACTIVE='true';
  
       COMMIT;
       
  /************************************ FILLING ACTION POLICIES FOR ALL TECH ********************************************************/
     INSERT INTO LS_CCO_MT_ACTION_POLICIES (EXECUTIONGUID, EXECUTIONSTARTTIMESTAMP, PROFILEID, PROFILE_NAME, CATEGORY1, CATEGORY2, CATEGORY3, CATEGORY4, ACTION_R) 
 SELECT  V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP, PROFILEID,ProfileName,CATEGORY1,CATEGORY2,CATEGORY3,CATEGORY4,ACTION_R
       FROM XMLTABLE ('/ExecutionPlan/ExecutionPlanProfiles/ExecutionPlanProfile'
                      PASSING V_ROW_LS_CCO_SETTINGS.EXECUTIONPROFILE 
                      COLUMNS ProfileId NUMBER (12) PATH 'Profile/Id',
                              ProfileName VARCHAR2 (100 BYTE) PATH 'Profile/Name',
                              ProfileType VARCHAR2 (30 BYTE) PATH 'Profile/Type',
                              XmlGroups XMLTYPE PATH 'ProfileParameters/GenericProfile/Groups') pr
        CROSS JOIN
            XMLTABLE ('Groups/Group[GroupName="GeneralSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="ActionMappingPolicy"]/Fields/Field/TableRows/TableRow/XmlRowFields'
                      PASSING (XmlGroups)
                      COLUMNS 
                      CATEGORY1 varchar2(100) PATH '/XmlRowFields/XElement/c0',
                      CATEGORY2 varchar2(100) PATH '/XmlRowFields/XElement/c1',
                      CATEGORY3 varchar2(100) PATH '/XmlRowFields/XElement/c2',
                      CATEGORY4 varchar2(100) PATH '/XmlRowFields/XElement/c3',
                      ACTION_R varchar2(100) PATH '/XmlRowFields/XElement/c4'
                      ) 
                      f1;
    
  /***********************************************************************************************************************/
  
  INSERT INTO LS_CCO_MT_ARFCN_CATAGORIES( EXECUTIONGUID, EXECUTIONSTARTTIMESTAMP, PROFILEID, PROFILE_NAME, ARFCN_CATEGORY, ARFCN)
 SELECT  V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP, ProfileId, ProfileName, 'CATEGORY_1',ARFCN_LIST
       FROM XMLTABLE ('/ExecutionPlan/ExecutionPlanProfiles/ExecutionPlanProfile'
                      PASSING V_ROW_LS_CCO_SETTINGS.EXECUTIONPROFILE 
                      COLUMNS ProfileId NUMBER (12) PATH 'Profile/Id',
                              ProfileName VARCHAR2 (100 BYTE) PATH 'Profile/Name',
                              ProfileType VARCHAR2 (30 BYTE) PATH 'Profile/Type',
                              XmlGroups XMLTYPE PATH 'ProfileParameters/GenericProfile/Groups') pr
        CROSS JOIN
            XMLTABLE ('Groups/Group[GroupName="GeneralSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="CoverageLayerSettings"]/Fields/Field[Name="CAT1_ARFCN_LIST"]/Values'
                      PASSING (XmlGroups)
                      COLUMNS 
                      ARFCN_LIST number PATH '/Values'
                      ) 
                 f1;
                      
                      
 INSERT INTO LS_CCO_MT_ARFCN_CATAGORIES( EXECUTIONGUID, EXECUTIONSTARTTIMESTAMP, PROFILEID, PROFILE_NAME, ARFCN_CATEGORY, ARFCN)
 SELECT  V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP, ProfileId, ProfileName, 'CATEGORY_2',ARFCN_LIST
       FROM XMLTABLE ('/ExecutionPlan/ExecutionPlanProfiles/ExecutionPlanProfile'
                      PASSING V_ROW_LS_CCO_SETTINGS.EXECUTIONPROFILE 
                      COLUMNS ProfileId NUMBER (12) PATH 'Profile/Id',
                              ProfileName VARCHAR2 (100 BYTE) PATH 'Profile/Name',
                              ProfileType VARCHAR2 (30 BYTE) PATH 'Profile/Type',
                              XmlGroups XMLTYPE PATH 'ProfileParameters/GenericProfile/Groups') pr
        CROSS JOIN
            XMLTABLE ('Groups/Group[GroupName="GeneralSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="CoverageLayerSettings"]/Fields/Field[Name="CAT2_ARFCN_LIST"]/Values'
                      PASSING (XmlGroups)
                      COLUMNS 
                      ARFCN_LIST number PATH '/Values'
                      ) 
                      f1;
                      
                      
   INSERT INTO LS_CCO_MT_ARFCN_CATAGORIES( EXECUTIONGUID, EXECUTIONSTARTTIMESTAMP, PROFILEID, PROFILE_NAME, ARFCN_CATEGORY, ARFCN)
 SELECT  V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP, ProfileId, ProfileName, 'CATEGORY_3',ARFCN_LIST
       FROM XMLTABLE ('/ExecutionPlan/ExecutionPlanProfiles/ExecutionPlanProfile'
                      PASSING V_ROW_LS_CCO_SETTINGS.EXECUTIONPROFILE 
                      COLUMNS ProfileId NUMBER (12) PATH 'Profile/Id',
                              ProfileName VARCHAR2 (100 BYTE) PATH 'Profile/Name',
                              ProfileType VARCHAR2 (30 BYTE) PATH 'Profile/Type',
                              XmlGroups XMLTYPE PATH 'ProfileParameters/GenericProfile/Groups') pr
        CROSS JOIN
            XMLTABLE ('Groups/Group[GroupName="GeneralSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="CoverageLayerSettings"]/Fields/Field[Name="CAT3_ARFCN_LIST"]/Values'
                      PASSING (XmlGroups)
                      COLUMNS 
                      ARFCN_LIST number PATH '/Values'
                      ) 
                      f1; 
                      
    INSERT INTO LS_CCO_MT_ARFCN_CATAGORIES( EXECUTIONGUID, EXECUTIONSTARTTIMESTAMP, PROFILEID, PROFILE_NAME, ARFCN_CATEGORY, ARFCN)
 SELECT  V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP, ProfileId, ProfileName, 'CATEGORY_4',ARFCN_LIST
       FROM XMLTABLE ('/ExecutionPlan/ExecutionPlanProfiles/ExecutionPlanProfile'
                      PASSING V_ROW_LS_CCO_SETTINGS.EXECUTIONPROFILE 
                      COLUMNS ProfileId NUMBER (12) PATH 'Profile/Id',
                              ProfileName VARCHAR2 (100 BYTE) PATH 'Profile/Name',
                              ProfileType VARCHAR2 (30 BYTE) PATH 'Profile/Type',
                              XmlGroups XMLTYPE PATH 'ProfileParameters/GenericProfile/Groups') pr
        CROSS JOIN
            XMLTABLE ('Groups/Group[GroupName="GeneralSettings"]/Tabs/Tab[Name="CF"]/Sections/Section[Name="CoverageLayerSettings"]/Fields/Field[Name="CAT4_ARFCN_LIST"]/Values'
                      PASSING (XmlGroups)
                      COLUMNS 
                      ARFCN_LIST number PATH '/Values'
                      ) 
                      f1;
                      
  COMMIT; 
 END;
  
 PROCEDURE FILL_TEMP_RELATION_TABLE
 IS 
 BEGIN
    
    INSERT /*+ APPEND */ INTO LS_CCO_MT_ALL_RELS (EXECUTIONGUID, EXECUTIONSTARTTIMESTAMP, SNAPSHOTID, CLID, SITENAME, BSID, CELLID, CELL, SECTORID, ARFCN, BEAMWIDTH, 
                                                     AZIMUTH, LATITUDE, LONGITUDE,ISTARGET, ISEXCLUDED, NCLID, NSITENAME, NBSID, NCELLID, NCELL, NSECTORID, NARFCN, NBEAMWIDTH, 
                                                     NAZIMUTH, NLATITUDE, NLONGITUDE, HO_ATTEMPT,NISTARGET, NISEXCLUDED,ACTIVE,DISTANCE
                                                    )
   select /* FULL(LC4C)  FULL(NPLSC)  FULL(PLRK)  FULL(PLSR)  */
     LC4C.EXECUTIONGUID, LC4C.EXECUTIONSTARTTIMESTAMP,PLRK.SNAPSHOTID,
     LC4C.CLID, LC4C.CM5 AS SITENAME, LC4C.BASESTATIONID AS BSID, LC4C.OBJECTKEY1 AS CELLID, LC4C.OBJECTNAME AS CELLID, 
     TO_NUMBER(LC4C.CM6) AS SECTORID, 
     LC4C.ARFCN, LC4C.BEAMWIDTH, LC4C.AZIMUTH, LC4C.LATITUDE, LC4C.LONGITUDE, LC4C.ISTARGET, LC4C.ISEXCLUDED,
     LC4C.CLID AS NCLID, NPLSC.CM5 AS NSITENAME, NPLSC.BASESTATIONID AS NBSID, NPLSC.OBJECTKEY1 AS NCELLID, NPLSC.OBJECTNAME AS NCELL ,
     TO_NUMBER(NPLSC.CM6) AS NSECTORID, 
     NPLSC.ARFCN AS NARFCN, NPLSC.BEAMWIDTH AS NBEAMWIDTH, NPLSC.AZIMUTH AS NAZIMUTH, NPLSC.LATITUDE AS NLATITUDE,NPLSC.LONGITUDE AS NLONGITUDE,
     PLRK.KPI1 HO_ATTEMPT,
     NPLSC.ISTARGET, NPLSC.ISEXCLUDED,  PLSR.CM1 AS ACTIVE,  
     CASE WHEN (PLSR.CM2) IS NOT NULL THEN TO_NUMBER(PLSR.CM2)  ELSE CALCDISTANCE_JAVA(LC4C.LATITUDE, LC4C.LONGITUDE, NPLSC.LATITUDE, NPLSC.LONGITUDE) END AS DISTANCE
      FROM  PISON_LITESON_SNAPSHOT_CELL LC4C 
                    JOIN  PISON_LITESON_SNAPSHOT_REL PLSR
                        ON      PLSR.CELLID=LC4C.OBJECTKEY1 
                            AND PLSR.EXECUTIONGUID=LC4C.EXECUTIONGUID 
                            AND PLSR.EXECUTIONSTARTTIMESTAMP=LC4C.EXECUTIONSTARTTIMESTAMP 
                           AND PLSR.CM1='1' --CM1:ACTIVE :todo gokhan */
                    JOIN PISON_LITESON_SNAPSHOT_CELL NPLSC 
                        ON      NPLSC.EXECUTIONGUID=PLSR.EXECUTIONGUID
                            AND NPLSC.EXECUTIONSTARTTIMESTAMP=PLSR.EXECUTIONSTARTTIMESTAMP
                            AND NPLSC.OBJECTKEY1=PLSR.NCELLID  
                    JOIN PISON_LITESON_REL_KPIS PLRK
                        ON      PLRK.EXECUTIONGUID=PLSR.EXECUTIONGUID
                            AND PLRK.EXECUTIONSTARTTIMESTAMP =PLSR.EXECUTIONSTARTTIMESTAMP 
                            AND PLRK.CELLID=PLSR.CELLID
                            AND PLRK.NCELLID=PLSR.NCELLID  
                   WHERE        LC4C.CM6 IN ('1,','2','3','4','5','6','7','8','9','0')
                            AND NPLSC.CM6 IN ('1,','2','3','4','5','6','7','8','9','0')
                            AND LC4C.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
                            AND LC4C.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP 
                            ;
       commit; 

   COMMIT;
 END; 
  
 PROCEDURE FILL_CELL_ALL_KPIS
 IS
 v_sqlRowCount number;
 BEGIN 
 
    INSERT /*+ APPEND */ INTO LS_CCO_MT_CELL_ALL_KPIS  
    (EXECUTIONSTARTTIMESTAMP, EXECUTIONGUID, SNAPSHOTID, ISTARGET, ISEXCLUDED, CLID, PROFILEID, BASESTATIONID, BASESTATIONNAME, CELLID, CELL, ARFCN, LATITUDE, LONGITUDE, AZIMUTH, HEIGHT,
     BEAMWIDTH, TILT,MINTILT,MAXTILT, MINCPICHPOWER, MAXCPICHPOWER, CPICHPOWER, SITENAME, SECTORID, BAND, MNC, VENDORNAME, VENDORID, 
     /* 4G KPI */ CRITICAL_BAD_COVERAGE, BORDER_TRAFFIC,COVERAGEHOATTEMPTPERRAB, PS_TOTAL_CALL,PRB_UTILIZATION, CA_DATA_VOLUME,
     /* 3G KPI */ SHO_OVERHEAD,IRAT_ATTEMPT,IRAT_ACTIVITY_PER_CALL ,POWER_UTILIZATION,CODE_UTILIZATION,TOTAL_FAIL,CS_RAB_ATTEMPT,
     /* GENERAL*/ CELL_AVAILABILITY, DROP_RATE_VOICE,NUMBER_OF_DROPS_VOICE,DROP_RATE_DATA,NUMBER_OF_DROPS_DATA,RAB_ATTEMPT,VOICE_TRAFFIC,DATA_VOLUME
    ) 
WITH V_IRAT_ATTEMPT AS ( SELECT SNAPSHOTID, CELLID, SUM(HO_ATTEMPT) AS IRAT_ATTEMPT  
                            FROM LS_CCO_MT_ALL_RELS T 
                            WHERE T.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
                              AND T.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID 
                              AND T.CLID<>T.NCLID  
                            GROUP BY T.SNAPSHOTID, T.CELLID
                        )
    SELECT /*+ ORDERED FULL(LC4OS) FULL(ARFCN_MAP) FULL(PLSC) FULL(PLC4CK) USE_HASH(PLSC PLC4CK)  */ 
                PLSC.EXECUTIONSTARTTIMESTAMP,
                PLSC.EXECUTIONGUID,
                PLC4CK.SNAPSHOTID, 
                PLSC.ISTARGET, 
                PLSC.ISEXCLUDED,
                PLSC.CLID,
                PLSC.PROFILEID,
                PLSC.BASESTATIONID ,
                PLSC.BASESTATIONNAME ,
                PLSC.OBJECTKEY1 CELLID, 
                PLSC.OBJECTNAME CELL, 
                PLSC.ARFCN,
                PLSC.LATITUDE,
                PLSC.LONGITUDE,
                PLSC.AZIMUTH,
                PLSC.CM11 AS HEIGHT,
                MAX(PLSC.BEAMWIDTH) OVER(PARTITION BY PLSC.CM5, PLSC.CM6) AS BEAMWIDTH, /* SITENAME + SECTORID  PISON-8490*/
                PLSC.CM1 AS ETILT,
                PLSC.CM2 AS MINTILT,
                PLSC.CM3 AS MAXTILT,
                NULL AS MINCPICHPOWER,
                NULL AS MAXCPICHPOWER, 
                PLSC.CM4 AS CPICHPOWER,
                PLSC.CM5 AS SITENAME,
                PLSC.CM6 AS SECTORID,
                PLSC.CM7 AS BAND,  
                PLSC.CM8 AS MNC,   
                PLSC.VENDORNAME,
                PLSC.VENDORID, 
                /****** 4G *******************/ 
                100*PLC4CK.KPI7  AS CRITICAL_BAD_COVERAGE,
                100*PLC4CK.KPI8  AS BORDER_TRAFFIC,
                100*PLC4CK.KPI9  AS COVERAGEHOATTEMPTPERRAB,  
                PLC4CK.KPI12     AS PS_TOTAL_CALL,
                100*PLC4CK.KPI13 AS PRB_UTILIZATION,  
                PLC4CK.KPI16     AS CA_DATA_VOLUME,  
                /********* 3G *********/
                100*PLC4CK.KPI2  AS SHO_OVERHEAD,
                 V_IRAT_ATTEMPT.IRAT_ATTEMPT,
                 100* V_IRAT_ATTEMPT.IRAT_ATTEMPT/PLC4CK.KPI3 AS  IRAT_ACTIVITY_PER_CALL,--100*DECODE(V_IRAT_ATTEMPT.IRAT_ATTEMPT,0,NULL,V_IRAT_ATTEMPT.IRAT_ATTEMPT/PLC4CK.KPI3) AS  IRAT_ACTIVITY_PER_CALL,   
                PLC4CK.KPI14 AS POWER_UTILIZATION,  
                PLC4CK.KPI15 AS CODE_UTILIZATION,  
                PLC4CK.KPI18 AS TOTAL_FAIL,  --CAPACITY_FAILURES
                PLC4CK.KPI3  AS CS_RAB_ATTEMPT,
                 /********* GENERAL *********/
               100*PLC4CK.KPI1 AS CellAvailability,     -- 2G,3G,4G
               PLC4CK.KPI4     AS DROP_RATE_VOICE ,     -- 2G,3G
               PLC4CK.KPI5     AS NUMBER_OF_DROPS_VOICE,-- 2G,3G,
               PLC4CK.KPI10    AS DROP_RATE_DATA ,      -- 3G,4G
               PLC4CK.KPI11    AS NUMBER_OF_DROPS_DATA, -- 3G,4G
               CASE PLSC.CLID WHEN 322 THEN PLC4CK.KPI6 /* ERAB_ATTEMPT */ WHEN 321 THEN PLC4CK.KPI3 /* CS_RAB_ATTEMPT */ END AS RAB_ATTEMPT,
               PLC4CK.KPI17 AS VOICE_TRAFFIC, -- 2G,3G
               PLC4CK.KPI19 AS DATA_VOLUME  --
            FROM   PISON_LITESON_SNAPSHOT_CELL PLSC  
               JOIN PISON_LITESON_CELL_KPIS PLC4CK 
                    ON     PLSC.EXECUTIONSTARTTIMESTAMP=PLC4CK.EXECUTIONSTARTTIMESTAMP
                       AND PLSC.EXECUTIONGUID=PLC4CK.EXECUTIONGUID 
                       AND PLSC.OBJECTKEY1=PLC4CK.OBJECTKEY1  
             LEFT JOIN V_IRAT_ATTEMPT   ON V_IRAT_ATTEMPT.CELLID=PLC4CK.OBJECTKEY1 AND V_IRAT_ATTEMPT.SNAPSHOTID=PLC4CK.SNAPSHOTID
            WHERE 
                 /* PLSC.ISEXCLUDED=0  commented for o2 issue checked with ziya */ 
                PLSC.ISOMNI=0  
              AND PLSC.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
              AND PLSC.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP;  
              
        COMMIT;
        
     v_sqlRowCount := SQL%ROWCOUNT; 
    LITESON_HELPERS.FILL_LOG_TABLE(V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP, null, 'Filled LS_CCO_MT_CELL_ALL_KPIS', v_sqlRowCount);
  
 END;
 
  PROCEDURE FILL_CELL_ALL_ACTIONS
 IS
 BEGIN
 
  INSERT /*+ APPEND */ INTO   LS_CCO_MT_CELL 
                        (EXECUTIONSTARTTIMESTAMP,EXECUTIONGUID,SNAPSHOTID,ISTARGET,TECH_TYPE,CLID,PROFILEID,BSID,BASESTATION,CELLID,CELL,ARFCN_CATEGORY,ARFCN,
                        LATITUDE,LONGITUDE,AZIMUTH, BEAMWIDTH,
                        CPICHPOWER,MINCPICHPOWER,MAXCPICHPOWER,
                       SITENAME,SECTORID,BAND,MNC,VENDOR,VENDORID,
                       /* 4G KPI */ CRITICAL_BAD_COVERAGE, BORDER_TRAFFIC,COVERAGEHOATTEMPTPERRAB, PS_TOTAL_CALL,PRB_UTILIZATION, CA_DATA_VOLUME,
                       /* 3G KPI */ SHO_OVERHEAD,IRAT_ATTEMPT,IRAT_ACTIVITY_PER_CALL ,POWER_UTILIZATION,CODE_UTILIZATION,TOTAL_FAIL,CS_RAB_ATTEMPT,
                       /* GENERAL*/ CELL_AVAILABILITY, DROP_RATE_VOICE,NUMBER_OF_DROPS_VOICE,DROP_RATE_DATA,NUMBER_OF_DROPS_DATA,RAB_ATTEMPT,VOICE_TRAFFIC,DATA_VOLUME
                        ) 
    SELECT /*+ ORDERED FULL(LC4OS) FULL(ARFCN_MAP) FULL(CELLS)    */ 
                 CELLS.EXECUTIONSTARTTIMESTAMP,
                 CELLS.EXECUTIONGUID,
                 SNAPSHOTID,
                 ISTARGET,
                 TECH_TYPE,
                 CELLS.CLID,
                 ARFCN_MAP.PROFILEID,
                 BASESTATIONID ,
                 BASESTATIONNAME ,
                 CELLS.CELLID, 
                 CELL,
                 ARFCN_MAP.ARFCN_CATEGORY,
                 ARFCN_MAP.ARFCN,
                 LATITUDE,
                 LONGITUDE,
                 AZIMUTH, 
                 BEAMWIDTH, 
                 CPICHPOWER,
                 MINCPICHPOWER,
                 MAXCPICHPOWER, 
                 SITENAME,
                 SECTORID,
                 BAND,  
                 MNC,   
                 VENDORNAME,
                 VENDORID, 
                /****** 4G *******************/ 
                CRITICAL_BAD_COVERAGE,
                BORDER_TRAFFIC,
                COVERAGEHOATTEMPTPERRAB,  
                PS_TOTAL_CALL,
                PRB_UTILIZATION, 
                CA_DATA_VOLUME,  
                /********* 3G *********/
                SHO_OVERHEAD,
                IRAT_ATTEMPT,
                IRAT_ACTIVITY_PER_CALL,   
                POWER_UTILIZATION,  
                CODE_UTILIZATION,  
                TOTAL_FAIL,  --CAPACITY_FAILURES,
                CS_RAB_ATTEMPT, 
                 /********* GENERAL *********/
               CELL_AVAILABILITY,     -- 2G,3G,4G
               DROP_RATE_VOICE ,     -- 2G,3G
               NUMBER_OF_DROPS_VOICE,-- 2G,3G,
               DROP_RATE_DATA ,      -- 3G,4G
               NUMBER_OF_DROPS_DATA, -- 3G,4G
               RAB_ATTEMPT,
               VOICE_TRAFFIC, -- 2G,3G
               DATA_VOLUME  
            FROM LS_CCO_MT_CELL_ALL_KPIS CELLS 
                  JOIN LS_CCO_MT_ARFCN_CATAGORIES ARFCN_MAP 
                    ON      ARFCN_MAP.EXECUTIONGUID=CELLS.EXECUTIONGUID
                       AND  ARFCN_MAP.EXECUTIONSTARTTIMESTAMP=CELLS.EXECUTIONSTARTTIMESTAMP
                       AND  ARFCN_MAP.PROFILEID=CELLS.PROFILEID 
                       AND  ARFCN_MAP.ARFCN=CELLS.ARFCN 
                 JOIN LS_CCO_MT_GENERAL_SETTINGS LC4OS
                      ON    CELLS.EXECUTIONGUID=LC4OS.EXECUTIONGUID
                       AND  CELLS.EXECUTIONSTARTTIMESTAMP=LC4OS.EXECUTIONSTARTTIMESTAMP
                       AND  CELLS.PROFILEID=LC4OS.PROFILEID 
                       AND CELLS.CLID=LC4OS.CLID 
              WHERE  
                  LC4OS.CCO_ACTIVE='true'
              AND CELLS.SNAPSHOTID='ActionPeriod'  
              AND CELLS.ISTARGET=1 
              AND CELLS.ISEXCLUDED=0 
              AND LC4OS.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
              AND LC4OS.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP;  
              
            COMMIT;
           LITESON_HELPERS.FILL_LOG_TABLE(V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP, null, 'Filled LS_CCO_MT_CELL', SQL%ROWCOUNT); 
 
 END;
 
 PROCEDURE CALCULATE_TA_VALUES
 IS
 BEGIN
       
        INSERT   INTO LS_CCO_TEMP_HISTOGRAMS (CELLID, TAPC90_DIST,HISTOGRAMSUM)
             WITH  
            TAPC90_DIST_TABLE AS
            ( 
             SELECT /*+ materialize  ordered full(s) full(h)  OPT_PARAM('_optimizer_adaptive_plans','false') */   
              S.CELLID, 
              S.HISTOGRAMID, 
               AGG_HISTSUM(HISTOGRAMDATA) AS HISTOGRAMSUM_RAW,
              MEDIAN(s.MULTIPLIER) AS MULTIPLIER,
              MAX(S.DATETIME) AS MAX_DATETIME
             FROM ALL_HISTOGRAMS H         
             JOIN AGGREGATED_HISTOGRAMS S ON H.ID=S.HISTOGRAMID   
             JOIN LS_CCO_MT_CELL PLSC  ON  PLSC.CELLID = S.CELLID   AND PLSC.VENDORID = H.VENDOR_ID 
              WHERE H.ID IN (6,68,71,6001,31,75,76,96,2007,6002,5001) -- 3G =  71: Nokia, 6: ERI, 68: Huaweii , 6001 ZTE  
                                                                               -- 4G =  31: Ericsson, 75: ZTE, 76: Huawei, 96: Nokia 5001:ALU
               AND S.DATETIME >=    V_ROW_LS_CCO_SETTINGS.ROP_START_DATE  
               AND S.DATETIME <=   V_ROW_LS_CCO_SETTINGS.ROP_END_DATE 
               AND PLSC.EXECUTIONGUID           = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID 
               AND PLSC.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP 
             GROUP BY S.CELLID ,S.HISTOGRAMID
            ),
           RESULT1 AS 
            (
              SELECT   CELLID, HISTOGRAMID, HISTOGRAMSUM_RAW, MULTIPLIER,MAX_DATETIME,
                   AGGREGATOR.CALCPERCENTILE(HISTOGRAMSUM_RAW,90) AS TAPC90_DIST_PRE,
                   AGGREGATOR.HistogramBinSumCount(HISTOGRAMSUM_RAW) AS HISTOGRAMSUM
              FROM TAPC90_DIST_TABLE
            ),
           RESULT2 AS 
           (
            SELECT   CELLID,
                     TO_NUMBER(REGEXP_SUBSTR(MAPPING, '[^;]+', 1, TAPC90_DIST_PRE)) / 1000
                   * CASE WHEN  GENERATION='3G' AND VENDOR_ID = 1 /* ERI */  THEN  MULTIPLIER ELSE 1 END  
                  AS TAPC90_DIST,
                  HISTOGRAMSUM,
                  ROW_NUMBER() OVER(PARTITION BY CELLID ORDER BY MAX_DATETIME DESC ) AS ROW_NUM 
                FROM RESULT1  
                 JOIN ALL_HISTOGRAMS AH on RESULT1.HISTOGRAMID = AH.ID AND AH.IS_ACTIVE = 1
                WHERE TAPC90_DIST_PRE IS NOT NULL
            ) 
            SELECT CELLID, TAPC90_DIST,HISTOGRAMSUM FROM RESULT2 WHERE ROW_NUM=1;  
             
        LITESON_HELPERS.FILL_LOG_TABLE(V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,NULL, 'TA calculation finished. Row count: '|| SQL%ROWCOUNT);
 
 /***************************************************************************************************************/
  
  MERGE /*+ ORDERED FULL(LC4C) USE_HASH(LC4C,SRC) */ INTO LS_CCO_MT_CELL LC4C 
             USING LS_CCO_TEMP_HISTOGRAMS SRC 
             ON      (    SRC.CELLID = LC4C.CELLID
                      and LC4C.EXECUTIONGUID           = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID 
                      AND LC4C.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP 
                     )
            WHEN MATCHED THEN UPDATE SET 
            LC4C.TAPC90_DIST = SRC.TAPC90_DIST, 
            LC4C.HISTOGRAMSUM = SRC.HISTOGRAMSUM;
              
            COMMIT;
        LITESON_HELPERS.FILL_LOG_TABLE(V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,NULL, 'Finished to update TA data. Row count: '||SQL%ROWCOUNT);
 END; 
 
 PROCEDURE FILL_RELATIONS
 IS 
 BEGIN
 
   /* CALCDISTANCE_JAVA AND OTHER JAVA FUNCTIONS DOES NOT RUN AS PARALLEL EXECUTION THEN WE USED WITH CLAUSE TO MAKE SEPERATE QUERY FOR EACH LOGIC */
           
            INSERT /*+ APPEND */ INTO    LS_CCO_MT_REL (EXECUTIONGUID,EXECUTIONSTARTTIMESTAMP,CLID,
                                          SITENAME,BSID,SECTORID,CELLID,CELL,AZIMUTH,AZIMUTH_START,AZIMUTH_END,
                                          NCLID,NSITENAME,NBSID,NSECTORID,NCELLID,NCELL,NAZIMUTH,
                                          BEARING,DISTANCE,INTERSITE,MEDIAN_DISTANCE,SEES,SEES_OVERSHOOT,HO_ATTEMPT)
            WITH LS_CCO_4G_REL_TEMP1 AS 
            (
                SELECT /*+ MATERIALIZE ORDERED PARALLEL FULL(LC4OS) FULL(LC4C) FULL(REL_MAP) USE_HASH(LC4C LC4OS)  USE_HASH(LC4C REL_MAP) */
                    LC4C.CLID,
                    LC4C.BSID,
                    LC4C.SECTORID,
                    LC4C.CELLID,
                    LC4C.CELL,
                    REL_MAP.LATITUDE, 
                    REL_MAP.LONGITUDE,
                    REL_MAP.NCLID,
                    REL_MAP.NLATITUDE,
                    REL_MAP.NLONGITUDE,
                    REL_MAP.NBSID,
                    REL_MAP.NSECTORID,
                    REL_MAP.NCELLID,
                    REL_MAP.NCELL,
                    REL_MAP.SITENAME,
                    REL_MAP.NSITENAME, 
                    REL_MAP.DISTANCE,
                     /* DECODE(LC4C.BSID,NPLSC.BASESTATIONID,0,1) INTERSITE,  PISON-9442 */
                    ROUND(MOD(360+REL_MAP.AZIMUTH,360),2) AZIMUTH,
                    ROUND(MOD(360+REL_MAP.NAZIMUTH,360),2) NAZIMUTH,
                    ROUND(360+LC4C.BEAMWIDTH/2,2) HALF_BEAMWIDTH, 
                    MOD(ROUND(360+(LC4C.AZIMUTH - LC4C.BEAMWIDTH*(1-LC4OS.OVERSHOOT_ALLOWED_PERC)/2),2),360) AZIMUTH_START_OVERSHOOT,
                    MOD(ROUND(360+(LC4C.AZIMUTH - LC4C.BEAMWIDTH/2),2),360) AZIMUTH_START,
                    MOD(ROUND(360+(LC4C.AZIMUTH + LC4C.BEAMWIDTH*(1-LC4OS.OVERSHOOT_ALLOWED_PERC)/2),2),360) AZIMUTH_END_OVERSHOOT,
                    MOD(ROUND(360+(LC4C.AZIMUTH + LC4C.BEAMWIDTH/2),2),360) AZIMUTH_END,
                    REL_MAP.HO_ATTEMPT
                FROM   LS_CCO_MT_CELL LC4C 
                    INNER JOIN  LS_CCO_MT_GENERAL_SETTINGS LC4OS 
                        ON      LC4OS.EXECUTIONSTARTTIMESTAMP = LC4C.EXECUTIONSTARTTIMESTAMP
                            AND LC4OS.EXECUTIONGUID = LC4C.EXECUTIONGUID 
                            AND LC4OS.PROFILEID = LC4C.PROFILEID  
                            AND LC4C.CLID = LC4OS.CLID --TODO:GOKHAN OMER 
                    INNER JOIN LS_CCO_MT_ALL_RELS REL_MAP 
                        ON REL_MAP.CELLID = LC4C.CELLID 
                            AND REL_MAP.ARFCN = REL_MAP.NARFCN
                            AND REL_MAP.CLID = LC4C.CLID  --TODO:GOKHAN OMER 
                            --AND REL_MAP.SNAPSHOTID = LC4C.SNAPSHOTID 
                            AND REL_MAP.EXECUTIONSTARTTIMESTAMP = LC4C.EXECUTIONSTARTTIMESTAMP
                            AND REL_MAP.EXECUTIONGUID = LC4C.EXECUTIONGUID 
              WHERE  LC4OS.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
                 AND LC4OS.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID  
                 AND REL_MAP.NBEAMWIDTH <> 360 
                 AND REL_MAP.ACTIVE = 1
                 AND REL_MAP.HO_ATTEMPT > 0
                 AND REL_MAP.SNAPSHOTID = 'ActionPeriod'
            ), 
            LS_CCO_4G_REL_TEMP2 AS 
            (
                SELECT /*+ MATERIALIZE PARALLEL  */ 
                LS_CCO_4G_REL_TEMP1.*,
                 MOD(ROUND(360+GEO$TO_DEGREE(BEARING_2POINTS(LATITUDE,LONGITUDE,NLATITUDE,NLONGITUDE)),2),360) BEARING
                FROM LS_CCO_4G_REL_TEMP1
            ) 
            SELECT V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID,
               V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,
               CLID,
               SITENAME, 
               BSID,
               SECTORID,
               CELLID,
               CELL,
               AZIMUTH,
               AZIMUTH_START,
               AZIMUTH_END,
               NCLID,
               NSITENAME, 
               NBSID,
               NSECTORID,
               NCELLID,
               NCELL,
               NAZIMUTH,
               BEARING,
               DISTANCE,
               CASE WHEN DISTANCE>0.01 THEN 1 ELSE 0 END INTERSITE, /* PISON-9442 */
               MEDIAN(CASE WHEN DISTANCE>0 THEN DISTANCE ELSE NULL END) OVER (PARTITION BY CELL,CELLID) MEDIAN_DISTANCE,
               CASE 
                WHEN AZIMUTH_START < AZIMUTH_END AND BEARING BETWEEN AZIMUTH_START AND AZIMUTH_END THEN 1 
                WHEN AZIMUTH_START > AZIMUTH_END AND (BEARING > AZIMUTH_START OR BEARING < AZIMUTH_END) THEN 1
                ELSE 0 
               END SEES,
               CASE 
                WHEN AZIMUTH_START_OVERSHOOT < AZIMUTH_END_OVERSHOOT AND BEARING BETWEEN AZIMUTH_START_OVERSHOOT AND AZIMUTH_END_OVERSHOOT THEN 1 
                WHEN AZIMUTH_START_OVERSHOOT > AZIMUTH_END_OVERSHOOT AND (BEARING > AZIMUTH_START_OVERSHOOT OR BEARING < AZIMUTH_END_OVERSHOOT) THEN 1
                ELSE 0 
               END SEES_OVERSHOOT,
               HO_ATTEMPT
            FROM LS_CCO_4G_REL_TEMP2; 
    
   COMMIT;
           
      LITESON_HELPERS.FILL_LOG_TABLE(V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,NULL, 
                            'Finished to fill relation table. Row count: '|| SQL%ROWCOUNT);
    
 END;
 
 PROCEDURE UPDATE_RELATION_BASED_KPIS
 IS
 BEGIN
  /*********************** UPDATE ERAB AND RAB VALUES FROM RELATION BASED TO CELL BASED  FOR BOTH 3G, 4G **************************************/
    MERGE INTO LS_CCO_MT_CELL T 
            USING 
            (
             with v_data1 as
                (
                    SELECT LC4R.CELLID,LC4R.NCELLID,HO_ATTEMPT,NCELL.RAB_ATTEMPT,/*LC4C.PROFILEID,*/
                    ROW_NUMBER() OVER (PARTITION BY SCELL.CELLID ORDER BY  HO_ATTEMPT DESC, DISTANCE ASC,NCELL.RAB_ATTEMPT ASC NULLS LAST) RNK 
                    FROM LS_CCO_MT_REL LC4R 
                      JOIN LS_CCO_MT_CELL SCELL 
                        ON      LC4R.EXECUTIONGUID=SCELL.EXECUTIONGUID
                            AND LC4R.EXECUTIONSTARTTIMESTAMP=SCELL.EXECUTIONSTARTTIMESTAMP
                            AND LC4R.CELLID=SCELL.CELLID 
                       JOIN LS_CCO_MT_CELL_ALL_KPIS NCELL 
                        ON      LC4R.EXECUTIONGUID=NCELL.EXECUTIONGUID
                            AND LC4R.EXECUTIONSTARTTIMESTAMP=NCELL.EXECUTIONSTARTTIMESTAMP
                            AND LC4R.NCELLID=NCELL.CELLID 
                            AND NCELL.SNAPSHOTID='ActionPeriod' 
                   WHERE        LC4R.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
                            AND LC4R.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP 
                            AND INTERSITE=1
                            AND HO_ATTEMPT IS NOT NULL
                )  
                SELECT 
                    CELLID,ROUND(AVG(RAB_ATTEMPT),1) EBEST3NONCOSITE_RAB_ATTEMPT, COUNT(*) 
                 from v_data1 
                WHERE RNK<=3
                GROUP BY CELLID
            ) SRC  
         ON (T.CELLID=SRC.CELLID AND T.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID AND T.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP)
          WHEN MATCHED THEN UPDATE SET 
         T.BEST3NONCOSITE_RAB_ATTEMPT = SRC.EBEST3NONCOSITE_RAB_ATTEMPT,
         T.TOLERATED_TRAFFIC = CASE WHEN SRC.EBEST3NONCOSITE_RAB_ATTEMPT IS NULL OR SRC.EBEST3NONCOSITE_RAB_ATTEMPT = 0 THEN 0
                                        ELSE ROUND(((SRC.EBEST3NONCOSITE_RAB_ATTEMPT - T.RAB_ATTEMPT) / SRC.EBEST3NONCOSITE_RAB_ATTEMPT),2) * 100
                                  END
           ;
         
  LITESON_HELPERS.FILL_LOG_TABLE(V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,NULL, 
  'Updated LS_CCO_MT_CELL: BEST3NONCOSITE_RAB_ATTEMPT,TOLERATED_TRAFFIC  Row count: '|| SQL%ROWCOUNT);
          
    MERGE INTO LS_CCO_MT_CELL LC4C USING 
    (
        SELECT /*+ FULL(LC3R) */
            LC3R.CELLID,
            COUNT(*) ACT_NBRCNT,
            MEDIAN(LC3R.DISTANCE) ACT_NBR_DIST_MEDIAN
        FROM LS_CCO_MT_REL LC3R
        WHERE LC3R.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
          AND LC3R.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
          AND HO_ATTEMPT>10
          AND INTERSITE=1 
        GROUP BY CELLID
    ) SRC ON (SRC.CELLID=LC4C.CELLID AND LC4C.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID AND LC4C.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP)
    WHEN MATCHED THEN UPDATE SET 
        LC4C.ACT_NBRCNT=SRC.ACT_NBRCNT, 
        LC4C.ACT_NBR_DIST_MEDIAN = SRC.ACT_NBR_DIST_MEDIAN;
        
        COMMIT;
        
    LITESON_HELPERS.FILL_LOG_TABLE(V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP, null, 
    'Updated LS_CCO_MT_CELL: ACT_NBRCNT,ACT_NBR_DIST_MEDIAN data', SQL%ROWCOUNT);
        
 END;
 
 PROCEDURE FILL_GAP_OVERSHOOT_REPORT
 IS
 v_count number;
 BEGIN
 
 INSERT /*+ APPEND */ INTO LS_CCO_MT_ALL_REPORT 
      (  EXECUTIONGUID,  EXECUTIONSTARTTIMESTAMP,CLID,  PROCESS_TYPE, CELLID,   CELL,  NCELLID,  NCELL, HO_ATTEMPT, INTERSITE,   DISTANCE, TAPC90_DIST, ACT_NBR_DIST_MEDIAN,
         ACT_NBRCNT, INSIDE_ARC, INSIDE_ARC2, NEAREST_SITE_DISTANCE, NEAREST_SITE_RANK, PC90_NBRDIST_COUNTER, PC90_NBRCNT_COUNTER, PC90, SITE_COUNT_INSIDE_ARC, 
         SITE_COUNT_INSIDE_ARC2, PC90_NBRDIST_MEDIAN, AVG_DIST_BEST2SITE,BEST2BASESTATION_DIST,SITE_COUNT_INSIDE_PROTECT_DIST,MEDIAN_BEST2SITE_DIFF,ACTUAL_FOOTPRINT
      )
   WITH V_DATA_1 AS 
    (
        SELECT /*+ MATERIALIZE FULL(PLSC) */
            LC4C.CLID,
            LC4R.CELLID,
            LC4R.CELL,
            LC4R.NCELLID,
            LC4R.NCELL,
            LC4R.HO_ATTEMPT,
            LC4R.INTERSITE,
            LC4R.DISTANCE,
            LC4C.TAPC90_DIST, 
            LC4OS.COVERAGE_PROTECTION_MULTIPLIER,
            SEES,  
            SEES_OVERSHOOT,
         CASE WHEN HO_ATTEMPT>10 AND INTERSITE=1 THEN MEDIAN(DISTANCE) OVER (PARTITION BY LC4C.CELLID,CASE WHEN HO_ATTEMPT>10 AND INTERSITE=1 THEN 1 ELSE 0 END) ELSE NULL END AS ACT_NBR_DIST_MEDIAN,
         COUNT(CASE WHEN HO_ATTEMPT>10 AND INTERSITE=1 THEN HO_ATTEMPT ELSE 0 END) OVER (PARTITION BY LC4C.CELLID,CASE WHEN HO_ATTEMPT>10 AND INTERSITE=1 THEN 1 ELSE 0 END) AS ACT_NBRCNT,
         CASE WHEN INTERSITE=1 AND /*PISON-9372 */ DISTANCE < TAPC90_DIST*(1-LC4OS.OVERSHOOT_ALLOWED_PERC) AND SEES_OVERSHOOT=1 THEN 1 ELSE 0 END AS INSIDE_ARC_O,
         CASE WHEN LC4R.INTERSITE=1 AND LC4R.DISTANCE < TAPC90_DIST AND SEES=1 THEN 1 ELSE 0 END  AS INSIDE_ARC_G,
         CASE WHEN LC4R.DISTANCE < LEAST(LC4C.ACT_NBR_DIST_MEDIAN,LC4C.TAPC90_DIST) * LC4OS.COVERAGE_PROTECTION_MULTIPLIER AND SEES=1 THEN 1 ELSE 0 END AS INSIDE_ARC2_G,
         CASE WHEN LC4R.INTERSITE=1 AND /*PISON-9372 */ SEES = 1 THEN CASE WHEN DENSE_RANK () OVER (PARTITION BY LC4R.CELLID,SEES ORDER BY LC4R.DISTANCE ASC) <= 2 THEN LC4R.DISTANCE ELSE NULL END ELSE NULL END BEST2BASESTATION_DIST_G,
         CASE WHEN LC4R.INTERSITE=1 AND /*PISON-9372 */ LC4R.SEES_OVERSHOOT = 1 THEN CASE WHEN DENSE_RANK () OVER (PARTITION BY LC4R.CELLID,LC4R.SEES_OVERSHOOT ORDER BY LC4R.DISTANCE ASC) <= 2 THEN LC4R.DISTANCE ELSE NULL END ELSE NULL END BEST2BASESTATION_DIST_O,
         CASE WHEN DISTANCE < v_MaximumInsideArcDistance AND SEES_OVERSHOOT=1 THEN 1 ELSE 0 END  AS INSIDE_ARC2_O,
         CASE WHEN SEES_OVERSHOOT = 1 AND INTERSITE=1 THEN MIN(DISTANCE) OVER (PARTITION BY LC4R.CELLID,CASE WHEN SEES_OVERSHOOT = 1 AND INTERSITE=1 THEN 1 ELSE 0 END) ELSE NULL END AS NEAREST_SITE_DISTANCE_O,
         CASE WHEN SEES = 1 AND LC4R.INTERSITE=1 THEN MIN(LC4R.DISTANCE) OVER (PARTITION BY LC4R.CELLID,CASE WHEN SEES = 1 AND LC4R.INTERSITE=1 THEN 1 ELSE 0 END) ELSE NULL END  AS NEAREST_SITE_DISTANCE_G,
         CASE WHEN SEES = 1 AND LC4R.INTERSITE=1 THEN RANK() OVER (PARTITION BY LC4R.CELLID,NBSID,CASE WHEN SEES = 1 AND LC4R.INTERSITE=1 THEN 1 ELSE 0 END ORDER BY LC4R.DISTANCE ASC) ELSE NULL END AS NEAREST_SITE_RANK,
         CASE WHEN LC4R.INTERSITE=1 AND ROUND(DECODE((SUM(HO_ATTEMPT) OVER( PARTITION BY LC4R.CELLID,LC4R.INTERSITE ORDER BY LC4R.CELL DESC)),0,NULL,100 * SUM(HO_ATTEMPT) OVER( PARTITION BY LC4R.CELL ORDER BY LC4R.CELLID,HO_ATTEMPT DESC ROWS UNBOUNDED PRECEDING ) /(SUM(HO_ATTEMPT) OVER( PARTITION BY LC4R.CELL ORDER BY LC4R.CELL DESC ))),2)<90 THEN LC4R.DISTANCE END PC90_NBRDIST_COUNTER,
         CASE WHEN LC4R.INTERSITE=1 AND ROUND(DECODE((SUM(HO_ATTEMPT) OVER( PARTITION BY LC4R.CELLID,LC4R.INTERSITE ORDER BY LC4R.CELL DESC)),0,NULL,100 * SUM(HO_ATTEMPT) OVER( PARTITION BY LC4R.CELL ORDER BY LC4R.CELLID,HO_ATTEMPT DESC ROWS UNBOUNDED PRECEDING ) /(SUM(HO_ATTEMPT) OVER( PARTITION BY LC4R.CELL ORDER BY LC4R.CELL DESC ))),2)<90 THEN 1 ELSE 0 END PC90_NBRCNT_COUNTER,
         ROUND(DECODE((SUM(HO_ATTEMPT) OVER( PARTITION BY LC4R.CELL ORDER BY LC4R.CELL DESC )),0,NULL,100*SUM(HO_ATTEMPT) OVER( PARTITION BY LC4R.CELL ORDER BY LC4R.CELLID,HO_ATTEMPT DESC ROWS UNBOUNDED PRECEDING ) /(SUM(HO_ATTEMPT) OVER( PARTITION BY LC4R.CELL ORDER BY LC4R.CELL DESC ))),2) PC90, 
         COUNT(DISTINCT CASE WHEN INTERSITE=1 AND SEES_OVERSHOOT=1 AND  TAPC90_DIST*(1-LC4OS.OVERSHOOT_ALLOWED_PERC)>DISTANCE THEN NBSID ELSE NULL END) OVER (PARTITION BY LC4C.CELL)  AS  SITE_COUNT_INSIDE_ARC_O,
         CASE LC4OS.TECH_TYPE 
             WHEN '3G' THEN  
               COUNT(DISTINCT CASE WHEN LC4R.INTERSITE=1 AND LC4R.SEES=1 AND TAPC90_DIST>LC4R.DISTANCE THEN NBSID ELSE NULL END) OVER (PARTITION BY LC4R.CELL)
             WHEN '4G' THEN 
               COUNT(DISTINCT CASE WHEN LC4R.INTERSITE=1 AND LC4R.SEES=1 AND v_TA90ExtensionForGapDetection*TAPC90_DIST>LC4R.DISTANCE THEN NBSID ELSE NULL END) OVER (PARTITION BY LC4C.CELL)
         END AS SITE_COUNT_INSIDE_ARC_G,  
         COUNT(DISTINCT CASE WHEN SEES_OVERSHOOT=1 AND DISTANCE< v_MaximumInsideArcDistance THEN NBSID ELSE NULL END) OVER (PARTITION BY LC4C.CELL) AS SITE_COUNT_INSIDE_ARC2,
         COUNT(DISTINCT CASE WHEN LC4R.BSID <> LC4R.NBSID  AND LC4R.SEES=1 AND LC4R.DISTANCE < LEAST(LC4C.ACT_NBR_DIST_MEDIAN,LC4C.TAPC90_DIST) * LC4OS.COVERAGE_PROTECTION_MULTIPLIER THEN NBSID ELSE NULL END) OVER (PARTITION BY LC4R.CELL) AS SITE_COUNT_INSIDE_PROTECT_DIST --TODO
        FROM LS_CCO_MT_CELL LC4C  
            INNER JOIN LS_CCO_MT_GENERAL_SETTINGS LC4OS
                ON      LC4C.EXECUTIONGUID=LC4OS.EXECUTIONGUID
                    AND LC4C.EXECUTIONSTARTTIMESTAMP=LC4OS.EXECUTIONSTARTTIMESTAMP 
                    AND LC4C.PROFILEID=LC4OS.PROFILEID 
                    AND LC4C.CLID=LC4OS.CLID 
            INNER JOIN  LS_CCO_MT_REL LC4R
                ON      LC4C.EXECUTIONGUID=LC4R.EXECUTIONGUID
                    AND LC4C.EXECUTIONSTARTTIMESTAMP=LC4R.EXECUTIONSTARTTIMESTAMP
                    AND LC4C.CELLID=LC4R.CELLID 
                    AND LC4C.CLID=LC4R.CLID  -- TODO:GOKHAN OMER
         WHERE  LC4C.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
            AND LC4C.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP 
     ),
  V_DATA_2 
    AS 
    (
    select /*+ materialze */
         V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID,
         V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,
         CLID,
         CELLID,
         CELL,
         NCELLID,
         NCELL,
         HO_ATTEMPT,
         INTERSITE,
         DISTANCE,
         TAPC90_DIST,    
         ACT_NBR_DIST_MEDIAN,
         ACT_NBRCNT,
         INSIDE_ARC_O,
         INSIDE_ARC_G,
         INSIDE_ARC2_G,
         INSIDE_ARC2_O,
         NEAREST_SITE_DISTANCE_O,
         NEAREST_SITE_DISTANCE_G,
         NEAREST_SITE_RANK,
         PC90_NBRDIST_COUNTER,
         PC90_NBRCNT_COUNTER,
         PC90, 
         SITE_COUNT_INSIDE_ARC_O,
         SITE_COUNT_INSIDE_ARC_G,
         SITE_COUNT_INSIDE_ARC2, 
         BEST2BASESTATION_DIST_O,
         BEST2BASESTATION_DIST_G,
         SITE_COUNT_INSIDE_PROTECT_DIST,
         CASE WHEN PC90_NBRDIST_COUNTER IS NOT NULL THEN MEDIAN(PC90_NBRDIST_COUNTER) OVER (PARTITION BY CELLID) ELSE NULL END PC90_NBRDIST_MEDIAN,
         CASE WHEN BEST2BASESTATION_DIST_O IS NOT NULL THEN AVG(BEST2BASESTATION_DIST_O) OVER (PARTITION BY CELLID) ELSE NULL END AVG_DIST_BEST2SITE_O,
         CASE WHEN BEST2BASESTATION_DIST_G IS NOT NULL THEN AVG(BEST2BASESTATION_DIST_G) OVER (PARTITION BY CELLID) ELSE NULL END AVG_DIST_BEST2SITE_G
       from V_DATA_1
       ),
   V_DATA_GAP   
    AS 
    (
     SELECT  
         EXECUTIONGUID,
         EXECUTIONSTARTTIMESTAMP,
         CLID,
         'UNDERSHOOT' AS PROCESS_TYPE,
         CELLID,
         CELL,
         NCELLID,
         NCELL,
         HO_ATTEMPT,
         INTERSITE,
         DISTANCE,
         TAPC90_DIST,    
         ACT_NBR_DIST_MEDIAN,
         ACT_NBRCNT, 
         INSIDE_ARC_G,
         INSIDE_ARC2_G,  
         NEAREST_SITE_DISTANCE_G,
         NEAREST_SITE_RANK,
         PC90_NBRDIST_COUNTER,
         PC90_NBRCNT_COUNTER,
         PC90, 
         SITE_COUNT_INSIDE_ARC_G,
         SITE_COUNT_INSIDE_ARC2, 
         PC90_NBRDIST_MEDIAN,
         AVG_DIST_BEST2SITE_G,
         BEST2BASESTATION_DIST_G,
         SITE_COUNT_INSIDE_PROTECT_DIST,
         NULL AS MEDIAN_BEST2SITE_DIFF,
         NULL AS ACTUAL_FOOTPRINT
      FROM V_DATA_2
      ),
    V_OVERSOOT
    AS 
    (
     select 
         V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID,
         V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,
         CLID,
         'OVERSHOOT' AS PROCESS_TYPE,
         CELLID,
         CELL,
         NCELLID,
         NCELL,
         HO_ATTEMPT,
         INTERSITE,
         DISTANCE,
         TAPC90_DIST,    
         ACT_NBR_DIST_MEDIAN,
         ACT_NBRCNT,
         INSIDE_ARC_O,
         INSIDE_ARC2_G, 
         NEAREST_SITE_DISTANCE_O, 
         NEAREST_SITE_RANK,
         PC90_NBRDIST_COUNTER,
         PC90_NBRCNT_COUNTER, 
         PC90,
         SITE_COUNT_INSIDE_ARC_O, 
         SITE_COUNT_INSIDE_ARC2, 
         PC90_NBRDIST_MEDIAN,
         AVG_DIST_BEST2SITE_O,
         BEST2BASESTATION_DIST_O,
         SITE_COUNT_INSIDE_PROTECT_DIST,
          CASE   WHEN ACT_NBR_DIST_MEDIAN <= AVG_DIST_BEST2SITE_O THEN TAPC90_DIST-AVG_DIST_BEST2SITE_O
                 WHEN ACT_NBR_DIST_MEDIAN > AVG_DIST_BEST2SITE_O THEN LEAST(ACT_NBR_DIST_MEDIAN,TAPC90_DIST)-AVG_DIST_BEST2SITE_O
                ELSE NULL
          END AS MEDIAN_BEST2SITE_DIFF,
           LEAST(ACT_NBR_DIST_MEDIAN,TAPC90_DIST) AS ACTUAL_FOOTPRINT
      FROM V_DATA_2
    )
   select * from  V_OVERSOOT
   UNION ALL 
   select * from V_DATA_GAP;
    
     COMMIT;
    LITESON_HELPERS.FILL_LOG_TABLE(V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP, null, 'Filled FILL_GAP_OVERSHOOT_REPORT', SQL%ROWCOUNT);
   
 END;
 
 PROCEDURE UNDERSHOOT_CELL_PRE_ACTION
 IS
    StepSizeCG NUMBER:= 1;
    MinimumIRATAttempt NUMBER := 100; 
 
 BEGIN 
 
  INSERT INTO LS_CCO_MT_CELL_PRE_ACTION ( EXECUTIONGUID, EXECUTIONSTARTTIMESTAMP,PROCESS_TYPE, TECH_TYPE, CLID, PROFILEID, 
                                        PROFILE_NAME, BSID, BASESTATION, CELLID, CELL, ARFCN_CATEGORY,
                                        DELTA_TILT_FINAL, DELTA_PWR_FINAL, TOTAL_COST, TOLERATED_TRAFFIC, PRB_UTIL, ACT_NEI ,ISWEAKCOVERAGE
    )  
     WITH V_MAIN_GAP_RAW AS 
          (
          SELECT /*+ materialize  ORDERED USE_HASH(LC4C LC4OS) USE_HASH(LC4OS LC4R)  */ LC4OS.EXECUTIONGUID,  
               LC4OS.EXECUTIONSTARTTIMESTAMP,
               PROCESS_TYPE,
               LC4OS.TECH_TYPE,
               LC4OS.CLID,
               LC4OS.PROFILEID,
               LC4OS.PROFILE_NAME,
               LC4C.BSID, 
               LC4C.BASESTATION, 
               LC4C.CELLID,
               LC4C.CELL, 
               LC4C.ARFCN_CATEGORY,
               LC4R.TAPC90_DIST,
               LC4R.AVG_DIST_BEST2SITE,
               LC4R.ACT_NBR_DIST_MEDIAN, 
               LC4R.PC90_NBRDIST_MEDIAN,
               LC4R.PC90_NBRCNT_COUNTER,
               LC4R.SITE_COUNT_INSIDE_ARC, 
               LC4R.SITE_COUNT_INSIDE_PROTECT_DIST, 
               LC4OS.TILT_DELTA_MIN_UI,     
               LC4OS.TILT_DELTA_MAX_UI, 
               LC4OS.POWER_DELTA_MIN_UI, 
               LC4OS.POWER_DELTA_MAX_UI, 
               LC4OS.CARRIERPOWER_SWITCH, 
               LC4C.TOLERATED_TRAFFIC,
               LC4C.PRB_UTILIZATION,
               LC4R.ACT_NBRCNT,
               LC4R.NEAREST_SITE_DISTANCE,
               LC4C.BEST3NONCOSITE_RAB_ATTEMPT, 
               LC4C.RAB_ATTEMPT,
               LC4OS.UNDERSHOOT_TOLERATED_ERAB_DIF, 
               LC4OS.UNDERSHOOT_TOLERATED_TRAFF, 
               LC4OS.UNDERSHOOT_IRAT_PER_CALL,  
               LC4OS.UNDERSOOT_ALLOWED_PRB_UTIL,
               LC4C.IRAT_ATTEMPT,
               LC4C.IRAT_ACTIVITY_PER_CALL, 
                LC4OS.OVERSHOOT_SHO_OVERHEAD,
               LC4OS.UNDERSHOOT_SHO_OVERHEAD,
               LC4C.SHO_OVERHEAD ,
               HISTOGRAMSUM, 
               UNDERSHOOT_MAX_ACTIVE_NE,
               LC4C.ISWEAKCOVERAGE,
               LC4OS.UNDERSHOOTER_SWITCH,
               LC4OS.WEAK_COVERAGE_SWITCH,
                LC4C.VENDOR,
               LC4C.CRITICAL_BAD_COVERAGE , 
               UNDERSHOOT_CRITICAL_BAD_COV,
               LC4C.BORDER_TRAFFIC, 
               LC4OS.UNDERSHOOT_BORDER_TRAFFIC_RT, 
               LC4C.COVERAGEHOATTEMPTPERRAB,  
               LC4OS.UNDERSHOOT_ALLOWED_HO_PERCALL, 
               CASE  WHEN VENDOR NOT IN ('ERI','HWI') THEN 1 
                     WHEN VENDOR='ERI' AND CRITICAL_BAD_COVERAGE > UNDERSHOOT_CRITICAL_BAD_COV THEN 1
                     WHEN VENDOR='HWI' AND ( BORDER_TRAFFIC > UNDERSHOOT_BORDER_TRAFFIC_RT OR COVERAGEHOATTEMPTPERRAB > UNDERSHOOT_ALLOWED_HO_PERCALL  ) THEN 1
                     ELSE 0
                END AS VENDOR_BASED_KPI_FILTERS
           FROM LS_CCO_MT_CELL LC4C   
                INNER JOIN LS_CCO_MT_GENERAL_SETTINGS LC4OS
                ON      LC4C.EXECUTIONGUID=LC4OS.EXECUTIONGUID
                    AND LC4C.EXECUTIONSTARTTIMESTAMP=LC4OS.EXECUTIONSTARTTIMESTAMP 
                    AND LC4C.PROFILEID=LC4OS.PROFILEID  
                    AND LC4C.CLID=LC4OS.CLID                    
                INNER JOIN LS_CCO_MT_ALL_REPORT LC4R     
                ON      LC4R.EXECUTIONSTARTTIMESTAMP=LC4C.EXECUTIONSTARTTIMESTAMP
                    AND LC4R.EXECUTIONGUID=LC4C.EXECUTIONGUID
                    AND LC4R.CELLID=LC4C.CELLID 
                WHERE   
                        LC4OS.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
                    AND LC4OS.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID 
                    AND LC4OS.CCO_ACTIVE ='true'
                    AND LC4R.PROCESS_TYPE='UNDERSHOOT'
            ),
            V_GAP_WEAK_COVERAGE AS  ( SELECT *  FROM  V_MAIN_GAP_RAW  WHERE ISWEAKCOVERAGE=1 AND WEAK_COVERAGE_SWITCH='true'  ),
            V_MAIN_GAP_1 AS 
              (
               SELECT * FROM V_MAIN_GAP_RAW T
                WHERE   ISWEAKCOVERAGE=0 
                    AND ACT_NBRCNT < UNDERSHOOT_MAX_ACTIVE_NE
                    AND HISTOGRAMSUM > 100
                    AND SITE_COUNT_INSIDE_ARC <= 0 --SITE_COUNT_INSIDE_ARC
                    AND NEAREST_SITE_DISTANCE IS NOT NULL 
                    AND SITE_COUNT_INSIDE_PROTECT_DIST > 0 
                    AND UNDERSHOOTER_SWITCH='true' 
              ),
            V_GAP_4G AS  
              (
                SELECT * 
                    FROM  V_MAIN_GAP_1 
                     WHERE ISWEAKCOVERAGE=0 AND CLID=322 AND PRB_UTILIZATION <= UNDERSOOT_ALLOWED_PRB_UTIL 
                         AND  TOLERATED_TRAFFIC > UNDERSHOOT_TOLERATED_ERAB_DIF
                         AND VENDOR_BASED_KPI_FILTERS = 1
               ), 
             V_GAP_3G AS 
              (
                SELECT *  
                    FROM  V_MAIN_GAP_1 
                     WHERE  ISWEAKCOVERAGE=0 AND CLID=321 AND SHO_Overhead < UNDERSHOOT_SHO_OVERHEAD   
                    AND ( (IRAT_ACTIVITY_PER_CALL > UNDERSHOOT_IRAT_PER_CALL AND IRAT_ATTEMPT > MinimumIRATAttempt) OR 
                           TOLERATED_TRAFFIC > UNDERSHOOT_TOLERATED_TRAFF
                        )
               ),
             V_MERGE_TECHS AS
               ( 
                  SELECT * FROM V_GAP_3G 
                  UNION ALL
                  SELECT * FROM V_GAP_4G
                  UNION ALL
                  SELECT * FROM V_GAP_WEAK_COVERAGE
               ),
             V_CALCULATE_DELTA AS
               (  
                SELECT  T.* , 
                     NVL( 10*NORMALIZE(((NEAREST_SITE_DISTANCE -TAPC90_DIST )/(NEAREST_SITE_DISTANCE )),MIN((NEAREST_SITE_DISTANCE -TAPC90_DIST )/(NEAREST_SITE_DISTANCE )) 
                                        OVER(PARTITION BY CLID),MAX((NEAREST_SITE_DISTANCE- TAPC90_DIST)/(NEAREST_SITE_DISTANCE )) OVER( ), 1,10 ),0
                        )   
                   + NVL(NORMALIZE (PC90_NBRCNT_COUNTER ,MIN(PC90_NBRCNT_COUNTER ) OVER(),
                                     MAX( PC90_NBRCNT_COUNTER) OVER( ),1 ,10), 0 
                        ) AS  TOTAL_GAP_COST,
                      (NEAREST_SITE_DISTANCE-TAPC90_DIST)/NEAREST_SITE_DISTANCE AS  DELTA_NORM,
                      ROUND( NVL(
                            NORMALIZE((NEAREST_SITE_DISTANCE-TAPC90_DIST)/NEAREST_SITE_DISTANCE,
                            MIN((NEAREST_SITE_DISTANCE-TAPC90_DIST)/NEAREST_SITE_DISTANCE) OVER(),
                            MAX((NEAREST_SITE_DISTANCE-TAPC90_DIST)/NEAREST_SITE_DISTANCE) OVER(),TILT_DELTA_MIN_UI,TILT_DELTA_MAX_UI),
                            TILT_DELTA_MIN_UI
                                ),0
                           ) AS DELTA_TILT_NORM, 
                    ROUND(NVL(
                            NORMALIZE((NEAREST_SITE_DISTANCE-TAPC90_DIST)/NEAREST_SITE_DISTANCE,
                            MIN((NEAREST_SITE_DISTANCE-TAPC90_DIST)/NEAREST_SITE_DISTANCE) OVER(),
                            MAX((NEAREST_SITE_DISTANCE-TAPC90_DIST)/NEAREST_SITE_DISTANCE) OVER(),POWER_DELTA_MIN_UI,POWER_DELTA_MAX_UI),
                        POWER_DELTA_MIN_UI 
                             ),0
                         ) AS DELTA_PWR_NORM 
                FROM V_MERGE_TECHS T
               )
                SELECT
                     V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, 
                     V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP, 
                     PROCESS_TYPE,
                     TECH_TYPE, 
                     CLID,
                     PROFILEID,
                     PROFILE_NAME,
                     BSID,
                     BASESTATION,
                     CELLID,
                     CELL, 
                     ARFCN_CATEGORY,
                     ROUND(MAX(CASE 
                            WHEN DELTA_NORM>=0.85 AND DELTA_NORM<1.00 THEN LEAST(DELTA_TILT_NORM,GREATEST(60,TILT_DELTA_MIN_UI))
                            WHEN DELTA_NORM>=0.75 AND DELTA_NORM<0.85 THEN LEAST(DELTA_TILT_NORM,GREATEST(50,TILT_DELTA_MIN_UI))
                            WHEN DELTA_NORM>=0.65 AND DELTA_NORM<0.75 THEN LEAST(DELTA_TILT_NORM,GREATEST(40,TILT_DELTA_MIN_UI))
                            WHEN DELTA_NORM>=0.50 AND DELTA_NORM<0.65 THEN LEAST(DELTA_TILT_NORM,GREATEST(30,TILT_DELTA_MIN_UI))
                            WHEN DELTA_NORM>=0.30 AND DELTA_NORM<0.50 THEN LEAST(DELTA_TILT_NORM,GREATEST(20,TILT_DELTA_MIN_UI))
                            WHEN DELTA_NORM>=0    AND DELTA_NORM<0.30 THEN LEAST(DELTA_TILT_NORM,GREATEST(10,TILT_DELTA_MIN_UI))
                            WHEN ISWEAKCOVERAGE=1 THEN TILT_DELTA_MIN_UI  -- :TODO GOKHAN ENIS
                     END)/StepSizeCG)*StepSizeCG DELTA_TILT_FINAL,
                     ROUND(MAX(CASE 
                            WHEN DELTA_NORM>=0.85 AND DELTA_NORM<1.00 THEN LEAST(DELTA_PWR_NORM,GREATEST(30,POWER_DELTA_MIN_UI))
                            WHEN DELTA_NORM>=0.75 AND DELTA_NORM<0.85 THEN LEAST(DELTA_PWR_NORM,GREATEST(25,POWER_DELTA_MIN_UI))
                            WHEN DELTA_NORM>=0.65 AND DELTA_NORM<0.75 THEN LEAST(DELTA_PWR_NORM,GREATEST(20,POWER_DELTA_MIN_UI))
                            WHEN DELTA_NORM>=0.50 AND DELTA_NORM<0.65 THEN LEAST(DELTA_PWR_NORM,GREATEST(15,POWER_DELTA_MIN_UI))
                            WHEN DELTA_NORM>=0.30 AND DELTA_NORM<0.50 THEN LEAST(DELTA_PWR_NORM,GREATEST(10,POWER_DELTA_MIN_UI))
                            WHEN DELTA_NORM>=0    AND DELTA_NORM<0.30 THEN LEAST(DELTA_PWR_NORM,GREATEST(05,POWER_DELTA_MIN_UI))
                     END)/StepSizeCG)*StepSizeCG  AS DELTA_PWR_FINAL,
                     MAX(TOTAL_GAP_COST) TOTAL_GAP_COST, 
                     MIN(TOLERATED_TRAFFIC) AS TOLERATED_TRAFFIC,
                     MIN(PRB_UTILIZATION) AS PRB_UTIL,
                     MIN(ACT_NBRCNT) AS ACT_NEI  ,
                     MAX(ISWEAKCOVERAGE)
                  FROM V_CALCULATE_DELTA
                GROUP BY PROFILE_NAME,PROFILEID,BSID,BASESTATION,CELL,ARFCN_CATEGORY,CELLID,CLID,TECH_TYPE,PROCESS_TYPE
                ORDER BY TOTAL_GAP_COST DESC;   
        
    COMMIT;
      LITESON_HELPERS.FILL_LOG_TABLE(V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP, null, 'Executed UNDERSHOOT_CELL_PRE_ACTION ', SQL%ROWCOUNT);
 END; 

  PROCEDURE OVERSHOOT_CELL_PRE_ACTION
 IS
    StepSizeCG NUMBER:= 1;
    MinimumIRATAttempt NUMBER := 100; 

 BEGIN
 
 INSERT INTO LS_CCO_MT_CELL_PRE_ACTION  (  EXECUTIONGUID, EXECUTIONSTARTTIMESTAMP,PROCESS_TYPE, TECH_TYPE, CLID, PROFILEID, PROFILE_NAME, BSID, 
                                           BASESTATION, CELLID, CELL,ARFCN_CATEGORY, DELTA_TILT_FINAL, DELTA_PWR_FINAL, TOTAL_COST,ACT_NEI,
                                           ISBADQUALITY
                                        )
  WITH V_DATA_OVERSHOOT_1 AS 
    (  
      SELECT /*+ MATERIALIZE ORDERED USE_HASH(LC4C LC4OS) USE_HASH(LC4OS LC4R)  */ 
        NVL(10*NORMALIZE(MEDIAN_BEST2SITE_DIFF/decode(ACTUAL_FOOTPRINT,0,1,ACTUAL_FOOTPRINT),
                         MIN(MEDIAN_BEST2SITE_DIFF/decode(ACTUAL_FOOTPRINT,0,1,ACTUAL_FOOTPRINT)) OVER(/*PARTITION BY LC4OS.CLID*/),
                         MAX(MEDIAN_BEST2SITE_DIFF/decode(ACTUAL_FOOTPRINT,0,1,ACTUAL_FOOTPRINT)) OVER(/*PARTITION BY LC4OS.CLID*/),1,10
                        ),0
            )+
             NVL(NORMALIZE(PC90_NBRCNT_COUNTER,MIN(PC90_NBRCNT_COUNTER) OVER(/*PARTITION BY LC4OS.CLID*/),
             MAX(PC90_NBRCNT_COUNTER) OVER(/*PARTITION BY LC4OS.CLID*/ ORDER BY 1),1,10),0)+ 
             NVL(NORMALIZE(SITE_COUNT_INSIDE_ARC,MIN(SITE_COUNT_INSIDE_ARC) OVER(/*PARTITION BY LC4OS.CLID*/),
                           MAX(SITE_COUNT_INSIDE_ARC) OVER(/*PARTITION BY LC4OS.CLID*/),1,10
                           ),0
                )  AS TOTAL_OVERSHOOT_COST,
             MEDIAN_BEST2SITE_DIFF/decode(ACTUAL_FOOTPRINT,0,1,ACTUAL_FOOTPRINT) DELTA_NORM, 
             ROUND(NVL(NORMALIZE
                          (MEDIAN_BEST2SITE_DIFF/decode(ACTUAL_FOOTPRINT,0,1,ACTUAL_FOOTPRINT),
                             MIN(MEDIAN_BEST2SITE_DIFF/decode(ACTUAL_FOOTPRINT,0,1,ACTUAL_FOOTPRINT)) OVER(/*PARTITION BY LC4OS.CLID*/),
                             MAX(MEDIAN_BEST2SITE_DIFF/decode(ACTUAL_FOOTPRINT,0,1,ACTUAL_FOOTPRINT)) OVER(/*PARTITION BY LC4OS.CLID*/),
                             TILT_DELTA_MIN_UI,TILT_DELTA_MAX_UI
                          ),
                        ((LC4OS.TILT_DELTA_MIN_UI+LC4OS.TILT_DELTA_MAX_UI) / 2) 
                    ),2
            ) AS  DELTA_TILT_NORM,
       ROUND(NVL(NORMALIZE(
                            MEDIAN_BEST2SITE_DIFF/decode(ACTUAL_FOOTPRINT,0,1,ACTUAL_FOOTPRINT),
                            MIN(MEDIAN_BEST2SITE_DIFF/decode(ACTUAL_FOOTPRINT,0,1,ACTUAL_FOOTPRINT)) OVER(),
                            MAX(MEDIAN_BEST2SITE_DIFF/decode(ACTUAL_FOOTPRINT,0,1,ACTUAL_FOOTPRINT)) OVER(),POWER_DELTA_MIN_UI,POWER_DELTA_MAX_UI),
                            ((LC4OS.POWER_DELTA_MIN_UI+LC4OS.POWER_DELTA_MAX_UI) / 2) 
                ),2  
            ) AS DELTA_PWR_NORM,
       LC4OS.EXECUTIONGUID,
       LC4OS.EXECUTIONSTARTTIMESTAMP,
       LC4OS.TECH_TYPE,
       LC4OS.CLID,
       LC4OS.PROFILEID,
       LC4OS.PROFILE_NAME,
       LC4C.BSID,
       LC4C.BASESTATION,
       LC4C.CELLID,
       LC4C.CELL, 
       LC4OS.POWER_DELTA_MIN_UI,
       LC4OS.POWER_DELTA_MAX_UI,
       LC4OS.TILT_DELTA_MIN_UI,
       LC4OS.TILT_DELTA_MAX_UI, 
       LC4C.ARFCN_CATEGORY, 
       LC4R.TAPC90_DIST,
       LC4R.AVG_DIST_BEST2SITE,
       LC4R.ACT_NBR_DIST_MEDIAN,
       LC4R.PC90_NBRDIST_MEDIAN,
       LC4R.PC90_NBRCNT_COUNTER,
       LC4R.SITE_COUNT_INSIDE_ARC,
       LC4R.ACT_NBRCNT,
       LC4R.PROCESS_TYPE,  
       LC4OS.TILT_SWITCH,
       LC4OS.CARRIERPOWER_SWITCH ,   
       LC4C.PRB_UTILIZATION,
       LC4C.BEST3NONCOSITE_RAB_ATTEMPT,
       LC4C.RAB_ATTEMPT,
       LC4OS.UNDERSHOOT_TOLERATED_ERAB_DIF,
       LC4C.VENDOR,
       LC4C.CRITICAL_BAD_COVERAGE,
       LC4OS.UNDERSHOOT_BORDER_TRAFFIC_RT,
       LC4C.COVERAGEHOATTEMPTPERRAB,
       LC4OS.UNDERSHOOT_IRAT_PER_CALL,
       LC4C.BORDER_TRAFFIC,
       UNDERSHOOT_CRITICAL_BAD_COV,
       LC4OS.UNDERSOOT_ALLOWED_PRB_UTIL,
       LC4C.IRAT_ATTEMPT,
       LC4C.IRAT_ACTIVITY_PER_CALL, 
       LC4OS.OVERSHOOT_SHO_OVERHEAD,
       LC4C.SHO_OVERHEAD,
       LC4C.ISBADQUALITY,
       LC4C.ISWEAKCOVERAGE,
       LC4OS.BAD_QUALITY_SWITCH,
       LC4OS.OVERSHOOT_SWITCH,
       LEAST (LC4C.ACT_NBR_DIST_MEDIAN, LC4R.TAPC90_DIST) as AVG_DIST_BEST2SITE_COMP,
       (LC4R.TAPC90_DIST - LC4R.AVG_DIST_BEST2SITE) / decode(LC4R.TAPC90_DIST,0,1,LC4R.TAPC90_DIST) as ALLOWEDOVERSHOOTPERCENTAGE,
       LC4C.HISTOGRAMSUM,
       LC4OS.OVERSHOOT_MIN_ACTIVE_NE,
       LC4OS.OVERSHOOT_MIN_SITE_IN_AREA,
       LC4OS.CCO_AVAIL,
       LC4C.CELL_AVAILABILITY
  FROM LS_CCO_MT_CELL LC4C
       INNER JOIN LS_CCO_MT_GENERAL_SETTINGS LC4OS
          ON     LC4C.EXECUTIONSTARTTIMESTAMP = LC4OS.EXECUTIONSTARTTIMESTAMP
             AND LC4C.EXECUTIONGUID = LC4OS.EXECUTIONGUID
             AND LC4C.PROFILEID = LC4OS.PROFILEID
             AND LC4C.CLID = LC4OS.CLID
       INNER JOIN LS_CCO_MT_ALL_REPORT LC4R
          ON     LC4R.EXECUTIONSTARTTIMESTAMP = LC4C.EXECUTIONSTARTTIMESTAMP
             AND LC4R.EXECUTIONGUID = LC4C.EXECUTIONGUID
             AND LC4R.CELLID = LC4C.CELLID
    WHERE LC4R.PROCESS_TYPE='OVERSHOOT' 
       AND LC4OS.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
       AND LC4OS.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID 
    ),
   V_DATA_OVERSHOOT_2
    as 
    ( select * 
        from V_DATA_OVERSHOOT_1 t 
        where  
            AVG_DIST_BEST2SITE_COMP >  AVG_DIST_BEST2SITE  --TODO: Ask Enis
        AND ALLOWEDOVERSHOOTPERCENTAGE > 0  /* PISON-7996 LC4OS.AllowedOvershootPercentage */
        AND HISTOGRAMSUM > 100
        AND CELL_AVAILABILITY >= CCO_AVAIL
        AND ACT_NBRCNT > OVERSHOOT_MIN_ACTIVE_NE /*LC4OS.MinNumberofActiveNeighbors */
        AND SITE_COUNT_INSIDE_ARC >= OVERSHOOT_MIN_SITE_IN_AREA /*MINREQUIREDSITECNT  */
        AND AVG_DIST_BEST2SITE IS NOT NULL
        AND ACT_NBR_DIST_MEDIAN <> 0 
      ) ,
   V_COMBINE_TECHS AS 
   (
    select * from V_DATA_OVERSHOOT_2 WHERE OVERSHOOT_SWITCH = 'true' and ISBADQUALITY =0 AND CLID =322 
    UNION ALL
    select * from V_DATA_OVERSHOOT_2 WHERE OVERSHOOT_SWITCH = 'true' and ISBADQUALITY =0 AND CLID =321 AND SHO_OVERHEAD > OVERSHOOT_SHO_OVERHEAD 
    UNION ALL
    SELECT * FROM V_DATA_OVERSHOOT_1 WHERE ISBADQUALITY =1 AND ISWEAKCOVERAGE=0 AND BAD_QUALITY_SWITCH='true' 
   )  
    SELECT    
         V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID,
         V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,
         PROCESS_TYPE,
         TECH_TYPE,
         CLID,
         PROFILEID,
         PROFILE_NAME,
         BSID,
         BASESTATION,
         CELLID,
         CELL,
         ARFCN_CATEGORY,
         ROUND(MAX(CASE 
                WHEN DELTA_NORM>=0.85 AND DELTA_NORM<1.00 THEN LEAST(DELTA_TILT_NORM,GREATEST(60,TILT_DELTA_MIN_UI))
                WHEN DELTA_NORM>=0.75 AND DELTA_NORM<0.85 THEN LEAST(DELTA_TILT_NORM,GREATEST(50,TILT_DELTA_MIN_UI))
                WHEN DELTA_NORM>=0.65 AND DELTA_NORM<0.75 THEN LEAST(DELTA_TILT_NORM,GREATEST(40,TILT_DELTA_MIN_UI))
                WHEN DELTA_NORM>=0.50 AND DELTA_NORM<0.65 THEN LEAST(DELTA_TILT_NORM,GREATEST(30,TILT_DELTA_MIN_UI))
                WHEN DELTA_NORM>=0.30 AND DELTA_NORM<0.50 THEN LEAST(DELTA_TILT_NORM,GREATEST(20,TILT_DELTA_MIN_UI))
                WHEN DELTA_NORM>=0    AND DELTA_NORM<0.30 THEN LEAST(DELTA_TILT_NORM,GREATEST(10,TILT_DELTA_MIN_UI))
         END)/StepSizeCG)*StepSizeCG DELTA_TILT_FINAL,
         ROUND(MAX(CASE 
                WHEN DELTA_NORM>=0.85 AND DELTA_NORM<1.00 THEN LEAST(DELTA_PWR_NORM,GREATEST(30,POWER_DELTA_MIN_UI))
                WHEN DELTA_NORM>=0.75 AND DELTA_NORM<0.85 THEN LEAST(DELTA_PWR_NORM,GREATEST(25,POWER_DELTA_MIN_UI))
                WHEN DELTA_NORM>=0.65 AND DELTA_NORM<0.75 THEN LEAST(DELTA_PWR_NORM,GREATEST(20,POWER_DELTA_MIN_UI))
                WHEN DELTA_NORM>=0.50 AND DELTA_NORM<0.65 THEN LEAST(DELTA_PWR_NORM,GREATEST(15,POWER_DELTA_MIN_UI))
                WHEN DELTA_NORM>=0.30 AND DELTA_NORM<0.50 THEN LEAST(DELTA_PWR_NORM,GREATEST(10,POWER_DELTA_MIN_UI))
                WHEN DELTA_NORM>=0    AND DELTA_NORM<0.30 THEN LEAST(DELTA_PWR_NORM,GREATEST(05,POWER_DELTA_MIN_UI))  
         END)/StepSizeCG)*StepSizeCG DELTA_PWR_FINAL,
         MAX(TOTAL_OVERSHOOT_COST) TOTAL_OVERSHOOT_COST, 
         MIN(ACT_NBRCNT) AS ACT_NEI,
         MAX(ISBADQUALITY) AS ISBADQUALITY
     FROM V_COMBINE_TECHS
     GROUP BY PROFILE_NAME,PROFILEID,BSID,BASESTATION,CELL,ARFCN_CATEGORY,CELLID, TECH_TYPE,CLID,PROCESS_TYPE;
               
       COMMIT;
 
 END;

PROCEDURE FIND_CELL_POWER_TILT_REPORT
 IS  
    /*****************
    This method process both GAP and OVERSHOOT record for cells 
    This method process only POWER record for cells 
    ******************/
 BEGIN
 
  INSERT INTO LS_CCO_MT_CELL_ACTION_REPORT (EXECUTIONGUID,EXECUTIONSTARTTIMESTAMP,PROCESS_TYPE,CLID,NODEID,NODE,PARAMNAME,OLDVALUE,NEWVALUE,
                                            REASON, PROFILEID,ACTION,CELL,CELLID,ARFCN_CATEGORY,MINVALUE,MAXVALUE,
                                            DELTAVALUE,SKIP_REASON,ACT_NEI,TOTAL_COST,RETID,RETMONAME_SPLITTED,ISWEAKCOVERAGE,ISBADQUALITY
                                           )
with v_process_Ret_maps as
    ( 
        SELECT /*+ materialize */ DISTINCT RETID  FROM ALL_CELLS_RET T WHERE T.ACTIVE=1 AND T.CELLID IN
                         (SELECT SUB.CELLID FROM LS_CCO_MT_CELL_PRE_ACTION SUB 
                          WHERE  SUB.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID AND SUB.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
                         )
    )
  , V_RESULT2 AS
    (
         SELECT  /*+ ORDERED FULL(AR) FULL(ACR) USE_HASH(ARC AR) USE_HASH(map1 ACR) USE_HASH(cco_cells ACR) use_hash(SETTING CCO_CELLS) use_hash(PROC_CELLS CCO_CELLS) MATERIALIZE */
        AC.EXECUTIONGUID,
        AC.EXECUTIONSTARTTIMESTAMP,
        AC.PROFILEID,  
        AC.CLID,
        AC.BASESTATIONID, 
        AC.BASESTATIONNAME,
        PROC_CELLS.PROCESS_TYPE, 
        CCO_CELLS.ARFCN_CATEGORY, 
        AC.CELLID,
        AC.CELL,
        CASE WHEN PROC_CELLS.CELLID IS NOT NULL THEN 1 ELSE 0 END IS_CELL_HAS_ACTION,
        CASE WHEN  CCO_CELLS.ARFCN_CATEGORY IS NOT NULL THEN 1 ELSE 0 END IS_CELL_IN_CATEGORY,
        --------------------------------
        CASE WHEN PROC_CELLS.DELTA_TILT_FINAL IS NULL THEN 1 ELSE PROC_CELLS.DELTA_TILT_FINAL  END AS DELTA_TILT_FINAL, /* TODO:ENIS */
        AR.ETILT,  
        (GREATEST(NVL(AR.MINTILT,SETTING.TILT_MIN_UI),SETTING.TILT_MIN_UI)) AS MINTILT,
        (LEAST(NVL(AR.MAXTILT,SETTING.TILT_MAX_UI),SETTING.TILT_MAX_UI))    AS MAXTILT, 
        -------------------------------- 
        PROC_CELLS.DELTA_PWR_FINAL,
        AC.CPICHPOWER,   
        (GREATEST(NVL(AC.MINCPICHPOWER,SETTING.POWER_MIN_UI),SETTING.POWER_MIN_UI)) AS MINCARRIERPOWER,
        (LEAST(NVL(AC.MAXCPICHPOWER,SETTING.POWER_MAX_UI),SETTING.POWER_MAX_UI))    AS  MAXCARRIERPOWER,
        --------------------------------- 
        ACR.NMONAME AS RETMONAME_SPLITTED,
        ACR.RETID,
        TOTAL_COST,
        PROC_CELLS.ACT_NEI,
        CCO_CELLS.ISWEAKCOVERAGE, 
        CCO_CELLS.ISBADQUALITY,
        SETTING.TILT_SWITCH
        FROM V_PROCESS_RET_MAPS MAP1 
             JOIN ALL_CELLS_RET ACR ON (MAP1.RETID = ACR.RETID)
             JOIN ALL_RETS AR ON (AR.RETID=ACR.RETID)  
             JOIN LS_CCO_MT_CELL_ALL_KPIS AC ON (AC.CELLID=ACR.CELLID) 
        LEFT JOIN LS_CCO_MT_CELL CCO_CELLS 
           ON (CCO_CELLS.CELLID = ACR.CELLID  and CCO_CELLS.EXECUTIONSTARTTIMESTAMP = AC.EXECUTIONSTARTTIMESTAMP  AND CCO_CELLS.EXECUTIONGUID = AC.EXECUTIONGUID )
        LEFT JOIN LS_CCO_MT_GENERAL_SETTINGS SETTING 
           ON ( SETTING.PROFILEID = CCO_CELLS.PROFILEID AND CCO_CELLS.CLID = SETTING.CLID AND SETTING.EXECUTIONGUID = AC.EXECUTIONGUID AND SETTING.EXECUTIONSTARTTIMESTAMP = AC.EXECUTIONSTARTTIMESTAMP) 
        LEFT JOIN LS_CCO_MT_CELL_PRE_ACTION PROC_CELLS 
            ON (PROC_CELLS.CELLID = CCO_CELLS.CELLID AND PROC_CELLS.EXECUTIONGUID = AC.EXECUTIONGUID AND PROC_CELLS.EXECUTIONSTARTTIMESTAMP = AC.EXECUTIONSTARTTIMESTAMP ) 
        WHERE ACR.ACTIVE=1   
            AND AR.ACTIVE=1
            AND AC.SNAPSHOTID='ActionPeriod' 
            AND  AC.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
            AND AC.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
    ),
  V_TILT1 
    AS
    (   SELECT t.*,
        CASE   WHEN ISWEAKCOVERAGE  = 0 AND ISBADQUALITY=0 AND PROCESS_TYPE='OVERSHOOT'  THEN 'TOTAL_OVERSHOOT_COST '  || TO_cHAR(TOTAL_COST)
                WHEN ISWEAKCOVERAGE = 0 AND ISBADQUALITY=0 AND PROCESS_TYPE='UNDERSHOOT' THEN 'TOTAL_UNDERSHOOT_COST ' || TO_cHAR(TOTAL_COST)
                WHEN ISWEAKCOVERAGE = 1 AND PROCESS_TYPE='UNDERSHOOT' THEN 'WEAK_COVERAGE'     
                WHEN ISBADQUALITY   = 1 AND PROCESS_TYPE='OVERSHOOT' THEN 'BAD_QUALITY' END     AS DESCRIPTION,
                CASE WHEN IS_CELL_HAS_ACTION=1 THEN  
                        CASE WHEN  DELTA_TILT_FINAL IS NULL THEN   'CANT FOUND DELTA VALUE'
                        WHEN PROCESS_TYPE='OVERSHOOT'  AND ETILT >= MAXTILT THEN  'Max Tilt ('|| TO_CHAR(MAXTILT) || ') achieved - cannot tilt anymore'
                        WHEN PROCESS_TYPE='UNDERSHOOT' AND ETILT <= MINTILT THEN  'Min Tilt (' || TO_CHAR(MINTILT) || ') achieved - cannot tilt anymore' 
                        WHEN ETILT IS NULL THEN 'Missing Initial TILT Value'
                        ELSE  NULL -- CLEAR 
                        END
                   WHEN IS_CELL_IN_CATEGORY=1 THEN  'NO ACTION'
                   WHEN IS_CELL_HAS_ACTION=0 AND IS_CELL_IN_CATEGORY=0 THEN  'NULL'
                   ELSE 'CASE_FAIL_2_NEED_CHECK'
                END AS SKIP_REASON,      --:TODO GOKHAN   
                    CASE  PROCESS_TYPE 
                    WHEN 'OVERSHOOT' THEN  LEAST(ETILT+DELTA_TILT_FINAL,MAXTILT) 
                    WHEN 'UNDERSHOOT' THEN  GREATEST(ETILT-DELTA_TILT_FINAL,MINTILT) 
                    END AS  NEW_VALUE,
                    'TILT' AS PARAMNAME
        FROM V_RESULT2 t  where  TILT_SWITCH='true'   
    ) ,
  V_TILT2
     AS 
    ( SELECT  EXECUTIONGUID,EXECUTIONSTARTTIMESTAMP, PROCESS_TYPE, CLID, BASESTATIONID,  BASESTATIONNAME,PARAMNAME,ETILT,
             NEW_VALUE, 
             DESCRIPTION , 
             PROFILEID,
             CASE WHEN SKIP_REASON IS NOT NULL THEN 'SKIP' ELSE 'modify' END AS ACTION_TYPE ,
             CELL, CELLID, ARFCN_CATEGORY,  MINTILT,MAXTILT, DELTA_TILT_FINAL, SKIP_REASON,  ACT_NEI ,TOTAL_COST, RETID, RETMONAME_SPLITTED,ISWEAKCOVERAGE,ISBADQUALITY
        FROM V_TILT1 S1
    ),
  V_POWER1
    AS 
    (  SELECT t.*,
        CASE  PROCESS_TYPE WHEN 'OVERSHOOT'  THEN 'TOTAL_OVERSHOOT_COST ' WHEN 'UNDERSHOOT' THEN 'TOTAL_UNDERSHOOT_COST '  END   || TO_cHAR(TOTAL_COST) AS DESCRIPTION,
                    CASE 
                        WHEN IS_CELL_HAS_ACTION=1 AND DELTA_PWR_FINAL IS NULL THEN  'CANT FOUND DELTA VALUE' 
                        WHEN PROCESS_TYPE='OVERSHOOT'  AND CPICHPOWER <= MINCARRIERPOWER THEN  'ALREADY EQUAL TO OR UNDER MINIMUM CPICHPOWER ('||MINCARRIERPOWER||')'
                        WHEN PROCESS_TYPE='UNDERSHOOT' AND CPICHPOWER >= MAXCARRIERPOWER THEN 'ALREADY EQUAL TO OR OVER MAXIMUM CPICHPOWER ('||MAXCARRIERPOWER||')'
                        WHEN IS_CELL_HAS_ACTION=1 AND CPICHPOWER IS NULL THEN   'Missing Initial Carrier Power Value'
                        WHEN PROCESS_TYPE IS NULL THEN 'NO ACTION'
                        ELSE NULL
                END AS SKIP_REASON, 
                CASE  PROCESS_TYPE WHEN 'OVERSHOOT'  THEN LEAST(CPICHPOWER+DELTA_PWR_FINAL,MINCARRIERPOWER) 
                                    WHEN 'UNDERSHOOT' THEN GREATEST(CPICHPOWER-DELTA_PWR_FINAL,MAXCARRIERPOWER) 
                END AS  NEW_VALUE ,
                    'CPICHPOWER' AS PARAMNAME  
        FROM V_RESULT2 t 
    ),
   V_POWER2
    AS
    (   SELECT   EXECUTIONGUID,  EXECUTIONSTARTTIMESTAMP,  PROCESS_TYPE, CLID,  BASESTATIONID,  BASESTATIONNAME, PARAMNAME, CPICHPOWER,
                 NEW_VALUE,
                 DESCRIPTION || TOTAL_COST, 
                 PROFILEID,
                 CASE WHEN SKIP_REASON IS NOT NULL THEN 'SKIP' ELSE 'modify' END AS ACTION_TYPE ,
                 CELL, CELLID, ARFCN_CATEGORY, MINCARRIERPOWER, MAXCARRIERPOWER, DELTA_PWR_FINAL, SKIP_REASON, ACT_NEI , TOTAL_COST, RETID, RETMONAME_SPLITTED, null as ISWEAKCOVERAGE
                 FROM V_POWER1 S1
    ) 
      SELECT * FROM V_TILT2 /*
       UNION ALL
      SELECT * FROM V_POWER2*/ ;
                
       COMMIT;   
 END; 
   
 PROCEDURE FILL_RET_ACTION_REPORT 
 IS 
 BEGIN
 
    INSERT INTO LS_CCO_MT_RET_ACTION_REPORT (EXECUTIONGUID,EXECUTIONSTARTTIMESTAMP,PROFILEID,PROFILENAME,RETID,RETMONAME_SPLITTED,PARAMNAME,CURRENT_VALUE,NEW_VALUE,MIN_VALUE,MAX_VALUE,
    DELTA_VALUE,TOTAL_COST, CATEGORY_1_ACTION,CATEGORY_2_ACTION,CATEGORY_3_ACTION,CATEGORY_4_ACTION,FINAL_ACTION,CELLID_AGG_LIST)
  WITH V_DATA_1 
    AS  
    (  
    SELECT /*+  ORDERED USE_NL(r c)  */ 
          PROFILEID,PROFILENAME, ARFCN_CATEGORY ,  
          MAX(case when PROCESS_TYPE ='UNDERSHOOT' AND ACTION='modify' THEN 1 ELSE 0 END) AS UNDERSHOOT_EXISTS,
          MAX(case when PROCESS_TYPE ='OVERSHOOT'  AND ACTION='modify' THEN 1 ELSE 0 END) AS OVERSHOOT_EXISTS,
          MAX(case when  ACTION='SKIP' THEN 1 ELSE 0 END) AS NO_ACTION_EXISTS,
          AVG(DELTAVALUE) AVG_DELTA_BY_CATEGORY,
          AVG(TOTAL_COST) AVG_COST_BY_CATEGORY,
          RETMONAME_SPLITTED,
          PARAMNAME,
          RETID,
          MAX(OLDVALUE) AS ORJ_VALUE_TILT,
          NULL AS NEWVALUE,
          MIN(MINVALUE) AS MINVALUE,
          MAX(MAXVALUE) AS MAXVALUE,
          LISTAGG (CELLID, ';') WITHIN GROUP (ORDER BY RETID,CELLID DESC) AS CELLID_AGG_LIST1
        FROM LS_CCO_MT_CELL_ACTION_REPORT 
        WHERE PARAMNAME  IN('TILT') -- TODO:kontrol
         AND EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID 
         AND EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
        GROUP BY  ARFCN_CATEGORY,PROFILEID,PROFILENAME,RETID,PROCESS_TYPE,RETMONAME_SPLITTED,RETID  ,PARAMNAME
     ) 
     , V_DATA_2
     AS 
      (
         SELECT  T.*,
             CASE WHEN UNDERSHOOT_EXISTS=1 AND OVERSHOOT_EXISTS=1 THEN 'MIXED'
                  WHEN UNDERSHOOT_EXISTS=0 AND OVERSHOOT_EXISTS=0 AND NO_ACTION_EXISTS =1 THEN 'NO_ACT'
                  WHEN UNDERSHOOT_EXISTS=1 AND OVERSHOOT_EXISTS=0 THEN 'GAP'
                  WHEN OVERSHOOT_EXISTS=1 AND UNDERSHOOT_EXISTS=0 THEN 'OS'
             END CATEGORY_BASED_AGG_ACTION
         FROM V_DATA_1 T
       ) 
        SELECT 
              V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID,
              V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,
              PROFILEID,
              PROFILENAME, 
              RETID,
              RETMONAME_SPLITTED, 
              PARAMNAME,
              MAX(ORJ_VALUE_TILT) AS OLDVALUE,
              NULL AS NEWVALUE,
              MIN(MINVALUE) AS MINVALUE,
              MAX(MAXVALUE) AS MAXVALUE ,
              ROUND(MIN(AVG_DELTA_BY_CATEGORY)) AS DELTA_BY_RET,
              ROUND(MIN(AVG_COST_BY_CATEGORY),2)  AS COST_BY_RET ,
              NVL(MIN(CASE  ARFCN_CATEGORY WHEN 'CATEGORY_1' THEN CATEGORY_BASED_AGG_ACTION END),'NULL')  AS CATEGORY_1_ACTION,
              NVL(MIN(CASE  ARFCN_CATEGORY WHEN 'CATEGORY_2' THEN CATEGORY_BASED_AGG_ACTION  END),'NULL') AS CATEGORY_2_ACTION,
              NVL(MIN(CASE  ARFCN_CATEGORY WHEN 'CATEGORY_3' THEN CATEGORY_BASED_AGG_ACTION  END),'NULL') AS CATEGORY_3_ACTION,
              NVL(MIN(CASE  ARFCN_CATEGORY WHEN 'CATEGORY_4' THEN CATEGORY_BASED_AGG_ACTION END),'NULL')  AS CATEGORY_4_ACTION,
              NULL AS FINAL_ACTION,
              LISTAGG (CELLID_AGG_LIST1, ';') WITHIN GROUP (ORDER BY RETID) as CELLID_AGG_LIST2
          FROM V_DATA_2     
          GROUP BY PROFILEID,PROFILENAME, RETID,RETMONAME_SPLITTED,PARAMNAME; 
                    
        /********************************FIND RELATED OSS ACTION IN MAPPING *********************************************/
        
         MERGE INTO LS_CCO_MT_RET_ACTION_REPORT T  
           USING LS_CCO_MT_ACTION_POLICIES S 
             ON (    T.PROFILEID = S.PROFILEID 
                 AND T.CATEGORY_1_ACTION=S.CATEGORY1 
                 AND T.CATEGORY_2_ACTION=S.CATEGORY2 
                 AND T.CATEGORY_3_ACTION=S.CATEGORY3 
                 AND T.CATEGORY_4_ACTION=S.CATEGORY4 
                 AND T.EXECUTIONGUID = S.EXECUTIONGUID 
                 AND T.EXECUTIONSTARTTIMESTAMP = S.EXECUTIONSTARTTIMESTAMP 
                 AND T.EXECUTIONGUID = S.EXECUTIONGUID 
                 AND T.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
                 AND T.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP 
                 AND S.ACTION_R IN ('DOWNTILT','UPTILT')
                ) 
            WHEN MATCHED THEN UPDATE SET 
            T.FINAL_ACTION = S.ACTION_R,
            t.ACTION='modify',
            T.DIRECTION='FW',
            T.NEW_VALUE =   CASE ACTION_R WHEN 'UPTILT' THEN ROUND(GREATEST(CURRENT_VALUE-DELTA_VALUE,MIN_VALUE),2)
                                         WHEN 'DOWNTILT' THEN ROUND(LEAST(CURRENT_VALUE+DELTA_VALUE,MAX_VALUE),2)
                            END; 

        /**************************************SET SKIPPED NOT FOUND IN MAPPING ******************************************/
        
        UPDATE  LS_CCO_MT_RET_ACTION_REPORT D  SET SKIP_REASON = 'No Action found in mapping'  , NEW_VALUE = null, ACTION='SKIP',DIRECTION=NULL
        WHERE D.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID AND D.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP 
          and NOT EXISTS ( SELECT 1 FROM LS_CCO_MT_ACTION_POLICIES T 
                                    WHERE ( D.PROFILEID = T.PROFILEID AND D.CATEGORY_1_ACTION=T.CATEGORY1 
                                            AND D.CATEGORY_2_ACTION=T.CATEGORY2 AND D.CATEGORY_3_ACTION=T.CATEGORY3 AND D.CATEGORY_4_ACTION=T.CATEGORY4 
                                            AND  T.EXECUTIONGUID = D.EXECUTIONGUID AND T.EXECUTIONSTARTTIMESTAMP = D.EXECUTIONSTARTTIMESTAMP  
                                          )
                        );

   /******************************* IF NO ACTION FOR A RET THEN PUT INFORMATIVE SKIP  *********************************************/
        UPDATE LS_CCO_MT_RET_ACTION_REPORT D
            SET   SKIP_REASON = 'No any action for CELLS under same RET', NEW_VALUE =NULL, ACTION='SKIP' , DIRECTION=NULL,FINAL_ACTION=NULL
        WHERE D.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID AND D.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP 
            AND (        CATEGORY_1_ACTION IN ('NO_ACT','NULL') 
                    AND  CATEGORY_2_ACTION IN ('NO_ACT','NULL') 
                    AND  CATEGORY_3_ACTION IN ('NO_ACT','NULL')
                    AND  CATEGORY_4_ACTION IN ('NO_ACT','NULL')
                );
         
       /************ DETECT CLUSTERED RETS ******************/
         INSERT INTO LS_CCO_MT_CLUSTER_REPORT
(EXECUTIONGUID, EXECUTIONSTARTTIMESTAMP, CLUSTERSIZECELLCOUNT, SOURCE_RETID, TARGET_RETID, TOTAL_COST_SUM_BY_RET,HO_ATTEMPT_SUM_BY_RET, NBR_RNK)
        WITH V_SETTING AS (    SELECT  DISTINCT ClusterSizeCellCount, PROFILEID 
                                FROM  LS_CCO_MT_GENERAL_SETTINGS  S 
                                WHERE  S.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
                                  AND S.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
                                  AND S.CCO_CLUSTER_SWITCH='true'
                          ),
      V_DATA1 AS 
                     ( 
                      SELECT /*+  ORDERED USE_HASH(SCELL SCELL) USE_HASH(CELL_REL NCELL) */ 
                            LC4OS.ClusterSizeCellCount,
                            SCELL.RETID AS SOURCE_RETID,   
                            NCELL.RETID AS TARGET_RETID,  
                            SUM(CELL_REL.HO_ATTEMPT) AS SUM_HO_ATTEMPT,
                            SUM(SCELL.TOTAL_COST) AS TOTAL_COST,                   
                            ROW_NUMBER () OVER (PARTITION BY SCELL.RETID ORDER BY  sum(SCELL.TOTAL_COST) DESC NULLS LAST, AVG(CELL_REL.HO_ATTEMPT) DESC NULLS LAST) AS NBR_RNK 
                            FROM   V_SETTING LC4OS 
                              JOIN LS_CCO_MT_RET_ACTION_REPORT SRET   
                               ON  LC4OS.PROFILEID = SRET.PROFILEID  
                              JOIN LS_CCO_MT_CELL_ACTION_REPORT SCELL 
                               ON (SCELL.RETID = SRET.RETID AND SCELL.EXECUTIONGUID = SRET.EXECUTIONGUID AND SCELL.EXECUTIONSTARTTIMESTAMP = SRET.EXECUTIONSTARTTIMESTAMP )
                              JOIN   LS_CCO_MT_REL CELL_REL
                               ON (CELL_REL.EXECUTIONGUID=SCELL.EXECUTIONGUID   AND  CELL_REL.EXECUTIONSTARTTIMESTAMP=SCELL.EXECUTIONSTARTTIMESTAMP AND  CELL_REL.CELLID = SCELL.CELLID )
                            JOIN LS_CCO_MT_CELL_ACTION_REPORT NCELL
                               ON ( NCELL.CELLID=CELL_REL.NCELLID AND NCELL.EXECUTIONGUID = CELL_REL.EXECUTIONGUID AND NCELL.EXECUTIONSTARTTIMESTAMP = CELL_REL.EXECUTIONSTARTTIMESTAMP)
                        WHERE
                               SRET.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP 
                         AND  SRET.EXECUTIONGUID= V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
                         AND CELL_REL.SITENAME<>CELL_REL.NSITENAME  
                         AND SRET.FINAL_ACTION IN ('UPTILT','DOWNTILT')
                        GROUP BY LC4OS.ClusterSizeCellCount,NCELL.RETID,SCELL.RETID
                     )
                     SELECT  
                           V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID,
                           V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,
                           ClusterSizeCellCount,
                           SOURCE_RETID, 
                           TARGET_RETID,
                           TOTAL_COST AS TOTAL_COST_SUM_BY_RET,
                           SUM_HO_ATTEMPT AS HO_ATTEMPT_SUM_BY_RET,
                           NBR_RNK
                       FROM V_DATA1 
                       where NBR_RNK<=ClusterSizeCellCount ;
                       
       /************ FLAG CLUSTERED RETS ******************/             
                     
       UPDATE LS_CCO_MT_RET_ACTION_REPORT T SET ACTION='SKIP', SKIP_REASON='DUE TO CLUSTERING' 
            WHERE t.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
              AND t.EXECUTIONGUID =  V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID 
              AND T.SKIP_REASON IS NULL
              AND T.PROFILEID IN  (SELECT SUB.PROFILEID FROM LS_CCO_MT_GENERAL_SETTINGS SUB 
                                                WHERE     SUB.EXECUTIONSTARTTIMESTAMP = T.EXECUTIONSTARTTIMESTAMP
                                                      AND SUB.EXECUTIONGUID=T.EXECUTIONGUID
                                                      AND SUB.EXECUTIONSTARTTIMESTAMP = T.EXECUTIONSTARTTIMESTAMP 
                                                      AND SUB.CCO_CLUSTER_SWITCH='true'
                                  )
              AND t.RETID IN (SELECT S.TARGET_RETID FROM LS_CCO_MT_CLUSTER_REPORT S 
                                                    WHERE S.EXECUTIONSTARTTIMESTAMP = T.EXECUTIONSTARTTIMESTAMP
                                                      AND S.EXECUTIONGUID = T.EXECUTIONGUID
                                          ); 
                                                    
                   COMMIT;     
 
 END;
 
 PROCEDURE FILL_POWER_ACTION_REPORT
 IS
 BEGIN
 
  INSERT INTO LS_CCO_MT_POWER_ACTION_REPORT (EXECUTIONGUID,EXECUTIONSTARTTIMESTAMP,PROFILEID,PROFILENAME,CATEGORY_1_ACTION,CATEGORY_2_ACTION,CATEGORY_3_ACTION,CATEGORY_4_ACTION,
                                             FINAL_ACTION,CELLID,CELL,PARAMNAME,CURRENT_POWER,NEW_POWER,MIN_POWER,MAX_POWER,DELTA_POWER,SKIP_REASON,REASON,ACTION,RETID,RETMONAME_SPLITTED
                                            )
    SELECT RET_REPT.EXECUTIONGUID, 
    RET_REPT.EXECUTIONSTARTTIMESTAMP,
    RET_REPT.PROFILEID,
    CELL_REPT.PROFILENAME,
    RET_REPT.CATEGORY_1_ACTION,
    RET_REPT.CATEGORY_2_ACTION,
    RET_REPT.CATEGORY_3_ACTION,
    RET_REPT.CATEGORY_4_ACTION,
    RET_REPT.FINAL_ACTION,
    CELL_REPT.CELLID,
    CELL_REPT.CELL,
    CELL_REPT.PARAMNAME ,
    CELL_REPT.OLDVALUE  ,
    CELL_REPT.NEWVALUE ,
    CELL_REPT.MINVALUE,
    CELL_REPT.MAXVALUE,
    CELL_REPT.DELTAVALUE,
    CELL_REPT.SKIP_REASON,
    CELL_REPT.REASON ,
    CELL_REPT.ACTION,
    CELL_REPT.RETID,
    CELL_REPT.RETMONAME_SPLITTED
    FROM LS_CCO_MT_RET_ACTION_REPORT RET_REPT 
    JOIN LS_CCO_MT_CELL_ACTION_REPORT CELL_REPT ON (RET_REPT.RETID = CELL_REPT.RETID AND RET_REPT.PROFILEID = CELL_REPT.PROFILEID AND RET_REPT.EXECUTIONGUID=CELL_REPT.EXECUTIONGUID 
                                                        AND RET_REPT.EXECUTIONSTARTTIMESTAMP= CELL_REPT.EXECUTIONSTARTTIMESTAMP)
    WHERE RET_REPT.ACTION='modify' 
     AND CELL_REPT.PROCESS_TYPE IS NOT NULL  
    -- AND cell_rept.ACTION='modify'  
     AND  RET_REPT.FINAL_ACTION IN ('GAP_CPICH','OS_CPICH','ALL_CPICH')
     AND CELL_REPT.PARAMNAME='CPICHPOWER'
     AND RET_REPT.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
     AND RET_REPT.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID;
  
  COMMIT;
  END;
  
  PROCEDURE FILL_TILT_ACTION_REPORT
  IS 
  BEGIN
  
 INSERT INTO  LS_CCO_MT_TILT_ACTION_REPORT   (executionguid,executionstarttimestamp,profileid,profilename,category_1_action,category_2_action,category_3_action,category_4_action,
                                            final_action,paramname,current_tilt,new_tilt,min_tilt,max_tilt,delta_tilt,total_cost,action,retid,retmoname_splitted
                                            )
SELECT EXECUTIONGUID,
       EXECUTIONSTARTTIMESTAMP,
       PROFILEID,
       PROFILENAME,
       t.CATEGORY_1_ACTION,
       CATEGORY_2_ACTION,
       CATEGORY_3_ACTION,
       CATEGORY_4_ACTION,
       t.FINAL_ACTION,
       T.PARAMNAME,
       t.CURRENT_VALUE,
       NEW_VALUE,
       MIN_VALUE,
       MAX_VALUE,
       t.DELTA_VALUE,
       TOTAL_COST,
       t.ACTION,
       RETID,
       t.RETMONAME_SPLITTED
  FROM LS_CCO_MT_RET_ACTION_REPORT t
 WHERE     FINAL_ACTION IN ('UPTILT', 'DOWNTILT')
       AND T.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
       AND T.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP;
 
COMMIT;
 END;
 
 PROCEDURE FILL_ROLLBACK_ACTION_REPORT
 IS
 V_CLUSTER_SIZE CONSTANT NUMBER :=10;
 BEGIN 
 
INSERT /*+ append */ INTO LS_CCO_MT_CELL_ROLLBACK_KPIS
     (EXECUTIONGUID, EXECUTIONSTARTTIMESTAMP, TECH_TYPE, CLID, PROFILEID, BSID, BASESTATION, CELLID, CELL, ARFCN, AZIMUTH, BEAMWIDTH, TILT, MINTILT, MAXTILT, 
       CPICHPOWER, SITENAME, SECTORID, BAND, MNC, VENDOR, VENDORID, ISTARGET, CELL_AVAILABILITY_REF, CA_DATA_VOLUME_REF, IRAT_ATTEMPT_REF, TOTAL_FAIL_REF, PS_TOTAL_CALL_REF,     
       NUMBER_OF_DROPS_VOICE_REF, NUMBER_OF_DROPS_DATA_REF, RAB_ATTEMPT_REF, VOICE_TRAFFIC_REF, DATA_VOLUME_REF, CAPACITY_UTILIZATION_REF, CELL_AVAILABILITY_CONS, 
       CA_DATA_VOLUME_CONS, IRAT_ATTEMPT_CONS, TOTAL_FAIL_CONS, PS_TOTAL_CALL_CONS, NUMBER_OF_DROPS_VOICE_CONS, DROP_RATE_DATA_CONS, NUMBER_OF_DROPS_DATA_CONS, RAB_ATTEMPT_CONS, 
       VOICE_TRAFFIC_CONS, DATA_VOLUME_CONS, CAPACITY_UTILIZATION_CONS
      ) 
 SELECT /*+ ORDERED FULL(LC4OS) FULL(ARFCN_MAP) FULL(PLSC) FULL(PLC4CK) USE_HASH(PLSC PLC4CK)  */
                KPI_ROLLBACK_REF.EXECUTIONGUID,
                KPI_ROLLBACK_REF.EXECUTIONSTARTTIMESTAMP, 
                NULL AS TECH_TYPE,
                KPI_ROLLBACK_REF.CLID,
                KPI_ROLLBACK_REF.PROFILEID,
                KPI_ROLLBACK_REF.BASESTATIONID ,
                KPI_ROLLBACK_REF.BASESTATIONNAME,
                KPI_ROLLBACK_REF.CELLID, 
                KPI_ROLLBACK_REF.CELL, 
                KPI_ROLLBACK_REF.ARFCN, 
                KPI_ROLLBACK_REF.AZIMUTH,
                KPI_ROLLBACK_REF.BEAMWIDTH ,
                KPI_ROLLBACK_REF.TILT,
                KPI_ROLLBACK_REF.MINTILT,
                KPI_ROLLBACK_REF.MAXTILT,
                KPI_ROLLBACK_REF.CPICHPOWER,
                KPI_ROLLBACK_REF.SITENAME,
                KPI_ROLLBACK_REF.SECTORID,
                KPI_ROLLBACK_REF.BAND,
                KPI_ROLLBACK_REF.MNC,  
               -- KPI_ROLLBACK_REF.RETMONAME, 
                KPI_ROLLBACK_REF.VENDORname,
                KPI_ROLLBACK_REF.VENDORID, 
                KPI_ROLLBACK_REF.ISTARGET,
                ---------------- REFERANCE ROLLBACK PERIOD --------------  
                KPI_ROLLBACK_REF.CELL_AVAILABILITY       AS CELL_AVAILABILITY_REF,
                KPI_ROLLBACK_REF.CA_DATA_VOLUME          AS CA_DATA_VOLUME_REF,
                KPI_ROLLBACK_REF.IRAT_ATTEMPT            AS IRAT_ATTEMPT_REF,
                KPI_ROLLBACK_REF.TOTAL_FAIL              AS TOTAL_FAIL_REF,
                KPI_ROLLBACK_REF.PS_TOTAL_CALL           AS PS_TOTAL_CALL_REF, 
                KPI_ROLLBACK_REF.NUMBER_OF_DROPS_VOICE   AS NUMBER_OF_DROPS_VOICE_REF,
                KPI_ROLLBACK_REF.NUMBER_OF_DROPS_DATA    AS NUMBER_OF_DROPS_DATA_REF,
                KPI_ROLLBACK_REF.RAB_ATTEMPT             AS RAB_ATTEMPT_REF,
                KPI_ROLLBACK_REF.VOICE_TRAFFIC           AS VOICE_TRAFFIC_REF,
                KPI_ROLLBACK_REF.DATA_VOLUME             AS DATA_VOLUME_REF,
                CASE  KPI_ROLLBACK_REF.CLID  
                 WHEN 321 THEN GREATEST(KPI_ROLLBACK_REF.CODE_UTILIZATION,KPI_ROLLBACK_REF.POWER_UTILIZATION)
                 WHEN 322 THEN KPI_ROLLBACK_REF.PRB_UTILIZATION 
                END                                  AS CAPACITY_UTILIZATION_REF, --
                 ---------------- CONSIDERED ROLLBACK PERIOD --------------  
                KPI_ROLLBACK_CONS.CELL_AVAILABILITY       AS CELL_AVAILABILITY_CONS,
                KPI_ROLLBACK_CONS.CA_DATA_VOLUME          AS CA_DATA_VOLUME_CONS,--
                KPI_ROLLBACK_CONS.IRAT_ATTEMPT            AS IRAT_ATTEMPT_CONS,--
                KPI_ROLLBACK_CONS.TOTAL_FAIL              AS TOTAL_FAIL_CONS,--
                KPI_ROLLBACK_CONS.PS_TOTAL_CALL           AS PS_TOTAL_CALL_CONS, 
                KPI_ROLLBACK_CONS.NUMBER_OF_DROPS_VOICE   AS NUMBER_OF_DROPS_VOICE_CONS,--
                KPI_ROLLBACK_CONS.DROP_RATE_DATA          AS DROP_RATE_DATA_CONS,
                KPI_ROLLBACK_CONS.NUMBER_OF_DROPS_DATA    AS NUMBER_OF_DROPS_DATA_CONS,--
                KPI_ROLLBACK_CONS.RAB_ATTEMPT             AS RAB_ATTEMPT_CONS,--
                KPI_ROLLBACK_CONS.VOICE_TRAFFIC           AS VOICE_TRAFFIC_CONS,--
                KPI_ROLLBACK_CONS.DATA_VOLUME             AS DATA_VOLUME_CONS,--
                CASE  KPI_ROLLBACK_CONS.CLID 
                 WHEN 321 THEN GREATEST(KPI_ROLLBACK_CONS.CODE_UTILIZATION,KPI_ROLLBACK_CONS.POWER_UTILIZATION)
                 WHEN 322 THEN KPI_ROLLBACK_CONS.PRB_UTILIZATION 
                END                                  AS CAPACITY_UTILIZATION_CONS
           FROM  LS_CCO_MT_CELL_ALL_KPIS KPI_ROLLBACK_REF           
                JOIN  LS_CCO_MT_CELL_ALL_KPIS KPI_ROLLBACK_CONS  
                ON  KPI_ROLLBACK_REF.CELLID = KPI_ROLLBACK_CONS.CELLID 
                AND KPI_ROLLBACK_REF.EXECUTIONGUID = KPI_ROLLBACK_CONS.EXECUTIONGUID
                AND KPI_ROLLBACK_REF.EXECUTIONSTARTTIMESTAMP = KPI_ROLLBACK_CONS.EXECUTIONSTARTTIMESTAMP
          WHERE
              KPI_ROLLBACK_REF.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
          AND KPI_ROLLBACK_REF.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
          AND KPI_ROLLBACK_REF.SNAPSHOTID = 'RollbackPeriod'
          AND KPI_ROLLBACK_CONS.SNAPSHOTID = 'ConsideredRollbackPeriod' 
          ;

       COMMIT;   
     
   /********************************************************************************************************/
 
     INSERT INTO LS_CCO_MT_CELL_ROLLBACK_REPORT (
    EXECUTIONGUID, EXECUTIONSTARTTIMESTAMP, ETILT, MINTILT, MAXTILT, RETID, RET_SPLITTED, TECH_TYPE, CLID, PROFILEID, CELLID, CELL, SITENAME, SECTORID, 
    NUMBER_OF_DROPS_DATA_REF,  NUMBER_OF_DROPS_VOICE_REF, RAB_ATTEMPT_REF, VOICE_TRAFFIC_REF, CA_DATA_VOLUME_REF, TOTAL_FAIL_REF, PS_TOTAL_CALL_REF,
    CAPACITY_UTILIZATION_REF, IRAT_ATTEMPT_REF, NUMBER_OF_DROPS_DATA_CONS, NUMBER_OF_DROPS_VOICE_CONS, RAB_ATTEMPT_CONS, VOICE_TRAFFIC_CONS, 
    CA_DATA_VOLUME_CONS, TOTAL_FAIL_CONS, PS_TOTAL_CALL_CONS, CAPACITY_UTILIZATION_CONS, IRAT_ATTEMPT_CONS, AGG_NUMBER_OF_DROPS_DATA_REF,  AGG_DROP_RATE_DATA_REF, 
    AGG_NUMBER_OF_DROPS_VOICE_REF, AGG_RAB_ATTEMPT_REF, AGG_DROP_RATE_VOICE_REF, AGG_VOICE_TRAFFIC_REF, AGG_CA_DATA_VOLUME_REF, AGG_TOTAL_FAIL_REF, AGG_PS_TOTAL_CALL_REF,
    AGG_MAX_CAPACITY_UTIL_REF, AGG_IRAT_ATTEMPT_REF, AGG_IRAT_PER_CALL_REF, AGG_NUMBER_OF_DROPS_DATA_CONS, AGG_DROP_RAB_ATTEMPT_CONS, 
    AGG_DROP_RATE_DATA_CONS, AGG_NUMBER_OF_DROPS_VOICE_CONS, AGG_DROP_RATE_VOICE_CONS, AGG_VOICE_TRAFFIC_CONS, AGG_CA_DATA_VOLUME_CONS, 
    AGG_TOTAL_FAIL_CONS,AGG_PS_TOTAL_CALL_CONS, AGG_MAX_CAPACITY_UTIL_CONS,  AGG_IRAT_ATTEMPT_CONS, AGG_IRAT_PER_CALL_CONS
    )
   with v_data1 as 
        (
            SELECT  /*+ ORDERED */
             CELL.EXECUTIONGUID,CELL.EXECUTIONSTARTTIMESTAMP, 
             AR.ETILT, ar.MINTILT, ar.MAXTILT,ar.RETID, ar.MONAME as RET_SPLITTED,
             TECH_TYPE, CLID, CELL.PROFILEID,  CELL.CELLID, CELL.CELL, CELL.SECTORID,CELL.SITENAME,  
             /***************  REFERANCE ROLLBACK SUM BY RET ******************************/
             cell.NUMBER_OF_DROPS_DATA_REF,
             cell.NUMBER_OF_DROPS_VOICE_REF,
             cell.RAB_ATTEMPT_REF,
             cell.VOICE_TRAFFIC_REF,
             cell.CA_DATA_VOLUME_REF,
             cell.TOTAL_FAIL_REF,
             cell.PS_TOTAL_CALL_REF,
             cell.CAPACITY_UTILIZATION_REF,
             cell.IRAT_ATTEMPT_REF,
             /***************  CONSIDER ROLLBACK SUM BY RET ******************************/
             cell.NUMBER_OF_DROPS_DATA_CONS,
             cell.NUMBER_OF_DROPS_VOICE_CONS,
             cell.RAB_ATTEMPT_CONS,
             cell.VOICE_TRAFFIC_CONS,
             cell.CA_DATA_VOLUME_CONS,
             cell.TOTAL_FAIL_CONS,
             cell.PS_TOTAL_CALL_CONS,
             cell.CAPACITY_UTILIZATION_CONS,
             cell.IRAT_ATTEMPT_CONS
       from 
             LS_CCO_MT_ORIGINALVALUES ORJ_VALUES 
        JOIN ALL_RETS AR ON (ORJ_VALUES.RETID=AR.RETID) 
        JOIN ALL_CELLS_RET ACR ON (ACR.RETID=AR.RETID) 
        JOIN LS_CCO_MT_CELL_ROLLBACK_KPIS cell  ON (CELL.CELLID=ACR.CELLID) 
       where  cell.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID 
         AND cell.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
         AND ACR.ACTIVE=1 AND AR.ACTIVE=1
         AND ORJ_VALUES.EXECUTIONPLANID = V_ROW_LS_CCO_SETTINGS.EXECUTIONPLANID
       ),
       V_DATA2 
        AS (
        select       EXECUTIONGUID, EXECUTIONSTARTTIMESTAMP, ETILT, MINTILT, MAXTILT, RETID, RET_SPLITTED, TECH_TYPE, CLID, PROFILEID, CELLID, CELL, SITENAME, SECTORID,
             /***************  REFERANCE ROLLBACK SUM BY RET ******************************/
             NUMBER_OF_DROPS_DATA_REF,
             NUMBER_OF_DROPS_VOICE_REF,
             RAB_ATTEMPT_REF,
             VOICE_TRAFFIC_REF,
             CA_DATA_VOLUME_REF,
             TOTAL_FAIL_REF,
             PS_TOTAL_CALL_REF,
             CAPACITY_UTILIZATION_REF,
             IRAT_ATTEMPT_REF,
             /***************  CONSIDER ROLLBACK SUM BY RET ******************************/
             NUMBER_OF_DROPS_DATA_CONS,
             NUMBER_OF_DROPS_VOICE_CONS,
             RAB_ATTEMPT_CONS,
             VOICE_TRAFFIC_CONS,
             CA_DATA_VOLUME_CONS,
             TOTAL_FAIL_CONS,
             PS_TOTAL_CALL_CONS,
             CAPACITY_UTILIZATION_CONS,
             IRAT_ATTEMPT_CONS,
             /***************  REFERANCE ROLLBACK SUM BY RET ******************************/
              SUM(NUMBER_OF_DROPS_DATA_REF) OVER(PARTITION BY RETID )   AS AGG_NUMBER_OF_DROPS_DATA_REF,
              SUM(RAB_ATTEMPT_REF) OVER(PARTITION BY RETID ) AS AGG_RAB_ATTEMPT_REF,
              SUM(NUMBER_OF_DROPS_VOICE_REF) OVER(PARTITION BY RETID )   AS AGG_NUMBER_OF_DROPS_VOICE_REF,
              SUM(VOICE_TRAFFIC_REF) OVER(PARTITION BY RETID )   AS AGG_VOICE_TRAFFIC_REF,
              SUM(CA_DATA_VOLUME_REF) OVER(PARTITION BY RETID )   AS AGG_CA_DATA_VOLUME_REF,
              SUM(TOTAL_FAIL_REF) OVER(PARTITION BY RETID) AS AGG_TOTAL_FAIL_REF,
              SUM(PS_TOTAL_CALL_REF) OVER(PARTITION BY RETID) AS AGG_PS_TOTAL_CALL_REF, --
              MAX(CAPACITY_UTILIZATION_REF) OVER(PARTITION BY RETID) AS AGG_MAX_CAPACITY_UTIL_REF,
              SUM(IRAT_ATTEMPT_REF)    OVER(PARTITION BY RETID )   AS AGG_IRAT_ATTEMPT_REF,
                /***************  CONSIDER ROLLBACK SUM BY RET ******************************/
              SUM(NUMBER_OF_DROPS_DATA_CONS) OVER(PARTITION BY RETID )   AS AGG_NUMBER_OF_DROPS_DATA_CONS,
              SUM(RAB_ATTEMPT_CONS) OVER(PARTITION BY RETID ) AS AGG_DROP_RAB_ATTEMPT_CONS,
              SUM(NUMBER_OF_DROPS_VOICE_CONS) OVER(PARTITION BY RETID )   AS AGG_NUMBER_OF_DROPS_VOICE_CONS,
              SUM(VOICE_TRAFFIC_CONS) OVER(PARTITION BY RETID )   AS AGG_VOICE_TRAFFIC_CONS,
              SUM(CA_DATA_VOLUME_CONS) OVER(PARTITION BY RETID )   AS AGG_CA_DATA_VOLUME_CONS,
              SUM(TOTAL_FAIL_CONS) OVER(PARTITION BY RETID) AS AGG_TOTAL_FAIL_CONS,
              SUM(PS_TOTAL_CALL_CONS) OVER(PARTITION BY RETID) AS AGG_PS_TOTAL_CALL_CONS, --
              MAX(CAPACITY_UTILIZATION_CONS) OVER(PARTITION BY RETID) AS AGG_MAX_CAPACITY_UTIL_CONS,
              SUM(IRAT_ATTEMPT_CONS)    OVER(PARTITION BY RETID )   AS AGG_IRAT_ATTEMPT_CONS
        from v_data1 AR 
        )
        SELECT 
            EXECUTIONGUID, EXECUTIONSTARTTIMESTAMP, ETILT, MINTILT, MAXTILT, RETID, RET_SPLITTED, TECH_TYPE, CLID, PROFILEID, CELLID, CELL, SITENAME, SECTORID,
             /***************  REFERANCE ROLLBACK SUM BY RET ******************************/
             NUMBER_OF_DROPS_DATA_REF,
             NUMBER_OF_DROPS_VOICE_REF,
             RAB_ATTEMPT_REF,
             VOICE_TRAFFIC_REF,
             CA_DATA_VOLUME_REF,
             TOTAL_FAIL_REF,
             PS_TOTAL_CALL_REF,
             ROUND(CAPACITY_UTILIZATION_REF,3) as CAPACITY_UTILIZATION_REF,
             IRAT_ATTEMPT_REF,
             /***************  CONSIDER ROLLBACK SUM BY RET ******************************/
             NUMBER_OF_DROPS_DATA_CONS,
             NUMBER_OF_DROPS_VOICE_CONS,
             RAB_ATTEMPT_CONS,
             VOICE_TRAFFIC_CONS,
             CA_DATA_VOLUME_CONS,
             TOTAL_FAIL_CONS,
             PS_TOTAL_CALL_CONS,
             ROUND(CAPACITY_UTILIZATION_CONS,3) AS CAPACITY_UTILIZATION_CONS,
             IRAT_ATTEMPT_CONS,
        /***************  REFERANCE ROLLBACK SUM BY RET ******************************/
              ROUND(AGG_NUMBER_OF_DROPS_DATA_REF,3) AS AGG_NUMBER_OF_DROPS_DATA_REF,
              ROUND(AGG_NUMBER_OF_DROPS_DATA_REF / DECODE(AGG_PS_TOTAL_CALL_REF,0,NULL,AGG_PS_TOTAL_CALL_REF),3) * 100 AS AGG_DROP_RATE_DATA_REF,  -- TODO:GOKHAN
              AGG_NUMBER_OF_DROPS_VOICE_REF, 
              AGG_RAB_ATTEMPT_REF,
              ROUND(AGG_NUMBER_OF_DROPS_VOICE_REF / DECODE(AGG_RAB_ATTEMPT_REF,0,NULL,AGG_RAB_ATTEMPT_REF),3) * 100 AS AGG_DROP_RATE_VOICE_REF, 
              AGG_VOICE_TRAFFIC_REF,
              AGG_CA_DATA_VOLUME_REF,
              AGG_TOTAL_FAIL_REF,
              AGG_PS_TOTAL_CALL_REF,
              ROUND(AGG_MAX_CAPACITY_UTIL_REF,3) AS AGG_MAX_CAPACITY_UTIL_REF,
              AGG_IRAT_ATTEMPT_REF,
              ROUND(AGG_IRAT_ATTEMPT_REF / DECODE(AGG_RAB_ATTEMPT_REF,0,NULL,AGG_RAB_ATTEMPT_REF),3) * 100 AS AGG_IRAT_PER_CALL_REF,
              /**************  CONSIDER ROLLBACK SUM BY RET ******************************/
              ROUND(AGG_NUMBER_OF_DROPS_DATA_CONS,3) AS AGG_NUMBER_OF_DROPS_DATA_CONS,
              AGG_DROP_RAB_ATTEMPT_CONS,
              ROUND(AGG_NUMBER_OF_DROPS_DATA_CONS / DECODE(AGG_PS_TOTAL_CALL_CONS,0,NULL,AGG_PS_TOTAL_CALL_CONS),3) * 100 AS AGG_DROP_RATE_DATA_CONS, -- TODO:GOKHAN
              AGG_NUMBER_OF_DROPS_VOICE_CONS,
              ROUND(AGG_NUMBER_OF_DROPS_VOICE_CONS / DECODE(AGG_DROP_RAB_ATTEMPT_CONS,0,NULL,AGG_DROP_RAB_ATTEMPT_CONS),3) * 100 AS AGG_DROP_RATE_VOICE_CONS,
              AGG_VOICE_TRAFFIC_CONS,
              AGG_CA_DATA_VOLUME_CONS,
              AGG_TOTAL_FAIL_CONS,
              AGG_PS_TOTAL_CALL_CONS,
              ROUND(AGG_MAX_CAPACITY_UTIL_CONS,3) AS AGG_MAX_CAPACITY_UTIL_CONS,
              AGG_IRAT_ATTEMPT_CONS,
              ROUND(AGG_IRAT_ATTEMPT_CONS / DECODE(AGG_DROP_RAB_ATTEMPT_CONS,0,NULL,AGG_DROP_RAB_ATTEMPT_CONS),3) * 100 AS AGG_IRAT_PER_CALL_CONS
        FROM V_DATA2;
        
     /********************************************************************************************************/
    INSERT INTO LS_CCO_MT_TEMP_CLUSTER_KPIS (SITENAME, SECTORID, SUM_VOICE_TRAFFIC_REF, SUM_DATA_VOLUME_REF, SUM_VOICE_TRAFFIC_CONS, SUM_DATA_VOLUME_CONS)
     WITH V_HO_BY_SITES AS
      (  
       SELECT 
       S.SITENAME, SECTORID, 
       S.NSITENAME,  NSECTORID,
       SUM(HO_ATTEMPT) SUM_OF_HO_ATTEMPT ,
       ROW_NUMBER() OVER(PARTITION BY SITENAME, SECTORID ORDER BY SUM(HO_ATTEMPT) DESC NULLS LAST) AS ROW_ORDER
        FROM  LS_CCO_MT_REL s 
        WHERE S.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
            AND S.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
            AND S.CLID= S.NCLID /* TODO:ZIYA AND ENIS */
       GROUP BY S.SITENAME, SECTORID, S.NSITENAME,  NSECTORID
      ),
     V_ROLLBACK_KPIS AS
     ( 
     SELECT SITENAME,SECTORID, 
            SUM(VOICE_TRAFFIC_REF)  AS SUM_VOICE_TRAFFIC_REF ,  
            SUM(DATA_VOLUME_REF)    AS SUM_DATA_VOLUME_REF,
            SUM(VOICE_TRAFFIC_CONS) AS SUM_VOICE_TRAFFIC_CONS ,  
            SUM(DATA_VOLUME_CONS)   AS SUM_DATA_VOLUME_CONS
      FROM LS_CCO_MT_CELL_ROLLBACK_KPIS T
      WHERE T.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
        AND T.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
      GROUP BY SITENAME,SECTORID
     ),
     V_FINAL1 AS 
     (
      SELECT MAP.SITENAME,
            MAP.SECTORID,
            RB_KPIS.SITENAME AS NSITENAME,
            RB_KPIS.SECTORID AS NSECTORID,
             RB_KPIS.SUM_VOICE_TRAFFIC_REF,
             RB_KPIS.SUM_DATA_VOLUME_REF,
             RB_KPIS.SUM_VOICE_TRAFFIC_CONS,
             RB_KPIS.SUM_DATA_VOLUME_CONS
       FROM V_HO_BY_SITES MAP 
        JOIN V_ROLLBACK_KPIS RB_KPIS 
          ON  (MAP.NSITENAME=RB_KPIS.SITENAME AND MAP.NSECTORID=RB_KPIS.SECTORID) --TODO:INFO NSITENAME=
       WHERE ROW_ORDER <=V_CLUSTER_SIZE
       ) 
       SELECT 
        SITENAME, SECTORID, 
        SUM(SUM_VOICE_TRAFFIC_REF) ,
        SUM(SUM_DATA_VOLUME_REF),
        SUM(SUM_VOICE_TRAFFIC_CONS),
        SUM(SUM_DATA_VOLUME_CONS)
        FROM V_FINAL1
        GROUP BY SITENAME,SECTORID;

     /********************************************************************************************************/

       MERGE INTO LS_CCO_MT_CELL_ROLLBACK_REPORT T
        USING LS_CCO_MT_TEMP_CLUSTER_KPIS S
        ON (        T.SITENAME=S.SITENAME 
             AND T.SECTORID=S.SECTORID 
             AND t.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID 
             AND t.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
           )
       WHEN MATCHED THEN UPDATE SET 
        T.CLUSTER_DATA_VOLUME_REF = ROUND(S.SUM_DATA_VOLUME_REF,3),
        T.CLUSTER_VOICE_TRAFF_REF = ROUND(S.SUM_VOICE_TRAFFIC_REF,3),
        T.CLUSTER_DATA_VOLUME_CONS = ROUND(S.SUM_DATA_VOLUME_CONS,3),
        T.CLUSTER_VOICE_TRAFF_CONS = ROUND(S.SUM_VOICE_TRAFFIC_CONS,3) ;
 
    
 /****************CALCULATE THRESHOLD KPI VALUES FOR UI INPUT COMPARASION ***********************************************************/
   UPDATE  LS_CCO_MT_CELL_ROLLBACK_REPORT T SET
   ---------------------INCREMENT POSITIVE NUMBERS----------------------------------
        AGG_NUMBER_OF_DROP_DATA_DIFF_TH = (AGG_NUMBER_OF_DROPS_DATA_CONS - AGG_NUMBER_OF_DROPS_DATA_REF),
        AGG_DROP_DATA_RATE_TH             = ROUND(((AGG_DROP_RATE_DATA_CONS - AGG_DROP_RATE_DATA_REF) / DECODE(AGG_DROP_RATE_DATA_CONS,0,NULL,AGG_DROP_RATE_DATA_CONS)),3) * 100  ,
        ---------------------INCREMENT POSITIVE NUMBERS----------------------------------
        AGG_NUMBER_OF_DROPS_VOICE_DIFF_TH = (AGG_NUMBER_OF_DROPS_VOICE_CONS - AGG_NUMBER_OF_DROPS_VOICE_REF) ,
        AGG_VOICE_DROP_RATE_TH      = ROUND(((AGG_DROP_RATE_VOICE_CONS - AGG_DROP_RATE_VOICE_REF) / DECODE(AGG_DROP_RATE_VOICE_CONS,0,NULL,AGG_DROP_RATE_VOICE_CONS)),3) *100 ,
        ---------------------INCREMENT POSITIVE NUMBERS----------------------------------
        AGG_IRAT_ATTEMPT_DIFF_TH     = (AGG_IRAT_ATTEMPT_CONS - AGG_IRAT_ATTEMPT_REF),
        AGG_IRAT_PER_CALL_CONS_RATE_TH = ROUND(((AGG_IRAT_PER_CALL_CONS - AGG_IRAT_PER_CALL_REF) / DECODE(AGG_IRAT_PER_CALL_CONS,0,NULL,AGG_IRAT_PER_CALL_CONS)),3) *100,
        ---------------------DECREMENT NEGATIVE NUMBERS---------------------------------- 
        AGG_CA_DATAVOLUME_RATE_TH   = ROUND(((AGG_CA_DATA_VOLUME_CONS - AGG_CA_DATA_VOLUME_REF) / DECODE(AGG_CA_DATA_VOLUME_CONS,0,NULL,AGG_CA_DATA_VOLUME_CONS)),3) * 100 ,
        AGG_CLUSTER_VOICE_TRAFFIC_RATE_TH = ROUND(((CLUSTER_VOICE_TRAFF_CONS - CLUSTER_VOICE_TRAFF_REF) / DECODE(CLUSTER_VOICE_TRAFF_CONS,0,NULL,CLUSTER_VOICE_TRAFF_CONS)),3) *100 ,
           AGG_CLUSTER_DATA_VOLUME_RATE_TH   = ROUND(((CLUSTER_DATA_VOLUME_CONS - CLUSTER_DATA_VOLUME_REF) / DECODE(CLUSTER_DATA_VOLUME_CONS,0,NULL,CLUSTER_DATA_VOLUME_CONS)),3) * 100
     ---------------------------------------------------------------------- 
    WHERE T.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID AND T.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP;
  
     /****************COMPARE CALCULATED KPI WITH UI THRESHOLD INPUT THEN DECUDE N ***********************************************************/

    MERGE INTO LS_CCO_MT_CELL_ROLLBACK_REPORT T 
    USING ( 
            SELECT CELLID,RETID,
         CASE
           WHEN  kpis.AGG_NUMBER_OF_DROP_DATA_DIFF_TH > ROLLBACK_NUMBER_OF_PACKET_DROP and kpis.AGG_DROP_DATA_RATE_TH > ROLLBACK_PACKET_DROP_RATE
                 THEN 'PACKET DROP RATE INCREMENT'
           WHEN  kpis.AGG_NUMBER_OF_DROPS_VOICE_DIFF_TH > ROLLBACK_NUMBER_OF_VOICE_DROP AND  kpis.AGG_VOICE_DROP_RATE_TH > ROLLBACK_VOICE_DROP_RATE
                 THEN 'VOICE DROP RATE INCREMENT'
           WHEN  kpis.AGG_IRAT_ATTEMPT_DIFF_TH > ROLLBACK_IRAT_HO_ACTIVIT_CALL AND kpis.AGG_IRAT_PER_CALL_CONS_RATE_TH > ROLLBACK_NUMBER_IRAT_HO_ACT
                 THEN 'IRAT HANDOVER ACTIVITY INCREMENT'
           WHEN  kpis.AGG_CA_DATAVOLUME_RATE_TH < ROLLBACK_CA_DATAVOLUME
                 THEN  'CA DATA VOLUME DECREMENT'
           WHEN  kpis.AGG_CLUSTER_VOICE_TRAFFIC_RATE_TH < ROLLBACK_CLUSTER_VOICE_TRAFFIC
                 THEN  'Cluster Voice Traffic DECREMENT'
           WHEN  kpis.AGG_CLUSTER_DATA_VOLUME_RATE_TH < ROLLBACK_CLUSTER_DATA_VOLUME
                 THEN 'Cluster Data Volume DECREMENT'
           WHEN AGG_MAX_CAPACITY_UTIL_CONS > ROLLBACK_MAX_CAPACITY_FAILURE
                THEN 'Resource Utilization INCREMENT'
           WHEN  AGG_TOTAL_FAIL_REF > ROLLBACK_RESOURCE_UTIL
                 THEN  'Capacity Failures INCREMENT'
          END AS ROLLBACK_REASON
          FROM LS_CCO_MT_CELL_ROLLBACK_REPORT kpis
               INNER JOIN LS_CCO_MT_GENERAL_SETTINGS LC4OS
                   ON     kpis.EXECUTIONGUID = LC4OS.EXECUTIONGUID
                      AND kpis.EXECUTIONSTARTTIMESTAMP = LC4OS.EXECUTIONSTARTTIMESTAMP
                      AND kpis.PROFILEID = LC4OS.PROFILEID
                      AND kpis.CLID =LC4OS.CLID
              WHERE LC4OS.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID 
                AND LC4OS.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP 
                AND LC4OS.ROLLBACK_SWITCH='true'
        ) MAP 
      ON (      T.RETID=MAP.RETID
            AND T.CELLID=MAP.CELLID
            AND T.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID 
            AND T.EXECUTIONSTARTTIMESTAMP =V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP 
         )
      WHEN MATCHED THEN UPDATE SET 
      T.ROLLBACK_REASON = MAP.ROLLBACK_REASON;

        COMMIT; 
 END; 
 
 PROCEDURE DELETE_HIST_AFTER_ROLLBACK
 IS
 BEGIN
 
  DELETE FROM LS_CCO_MT_ORIGINALVALUES ST WHERE ST.EXECUTIONPLANID = V_ROW_LS_CCO_SETTINGS.EXECUTIONPLANID
    AND ST.RETMONAME_SPLITTED IN ( SELECT   T.RET_SPLITTED 
                                           FROM LS_CCO_MT_CELL_ROLLBACK_REPORT T 
                                           WHERE T.ROLLBACK_REASON IS NOT NULL 
                                             AND T.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
                                             AND T.EXECUTIONSTARTTIMESTAMP =V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP  
                            );
        COMMIT; 
 END;
 
 PROCEDURE FILL_ROLLBACK_SOLUTIONS
 IS 
 BEGIN
 
  INSERT INTO LS_CCO_MT_SOLUTIONS (EXECUTIONGUID,EXECUTIONSTARTTIMESTAMP, OBJECT_TYPE,OBJECTID, MONAME, PARAMNAME,
                                  CURRENT_TILT, NEW_TILT,ACTION,SKIP_REASON,RETMONAME_SPLITTED, DIRECTION
                    )                                 
                                      
    WITH V_MAP
         AS (SELECT DISTINCT T.RET_SPLITTED, ROLLBACK_REASON 
               FROM LS_CCO_MT_CELL_ROLLBACK_REPORT T 
               WHERE T.ROLLBACK_REASON IS NOT NULL 
                 AND T.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
                 AND T.EXECUTIONSTARTTIMESTAMP =V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP 
             )
    SELECT V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, 
           V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP, 
          'RET'            AS OBJECT_TYPE,
           C.RETID         AS OBJECTID,
           C.RETMONAME_SPLITTED AS MONAME,
             PARAMNAME,
           NULL            AS CURRENT_TILT, 
           C.ORIGINALVALUE AS NEW_TILT, 
           'modify'        AS ACTION,
           ROLLBACK_REASON AS SKIP_REASON,
           RETMONAME_SPLITTED AS RETMONAME_SPLITTED,
           'FW' AS DIRECTION
      FROM V_MAP 
      JOIN LS_CCO_MT_ORIGINALVALUES C ON (V_MAP.RET_SPLITTED = C.RETMONAME_SPLITTED)
      WHERE  C.EXECUTIONPLANID = V_ROW_LS_CCO_SETTINGS.EXECUTIONPLANID ; 
       
           COMMIT;  
 END;
 
 PROCEDURE FILL_ACTION_SOLUTIONS
 IS 
 BEGIN
 
 INSERT INTO LS_CCO_MT_SOLUTIONS (EXECUTIONGUID,EXECUTIONSTARTTIMESTAMP,PROFILENAME, CATEGORY_1_ACTION,CATEGORY_2_ACTION,CATEGORY_3_ACTION,CATEGORY_4_ACTION,FINAL_ACTION,
                                 OBJECT_TYPE,OBJECTID, MONAME, PARAMNAME,
                                  CURRENT_TILT, NEW_TILT,MIN_TILT,MAX_TILT,DELTA_TILT,ACTION,SKIP_REASON,RETMONAME_SPLITTED, DIRECTION,OSSCOMMAND
                                  )
     SELECT EXECUTIONGUID,EXECUTIONSTARTTIMESTAMP,PROFILENAME,
     CATEGORY_1_ACTION,CATEGORY_2_ACTION,CATEGORY_3_ACTION,CATEGORY_4_ACTION,FINAL_ACTION,
     'RET' AS OBJECT_TYPE,
     RETID OBJECTID,
     RETMONAME_SPLITTED AS MONAME,
     PARAMNAME,
     CURRENT_TILT, NEW_TILT,MIN_TILT,MAX_TILT,DELTA_TILT,
     ACTION,SKIP_REASON, RETMONAME_SPLITTED,'FW' AS DIRECTION,
     CASE PARAMNAME  
             WHEN 'TILT' THEN  
                'modret('  || AR.RETID  || ',"' || AR.RETMONAME_SPLITTED || '",' || ar.PARAMNAME || '=' || TO_CHAR(ar.NEW_TILT) ||') // ' || 'FW' ||':'||ar.SKIP_REASON
             WHEN 'CPICHPOWER'  THEN
                'modcell(' ||  ar.RETID || ',' || ar.PARAMNAME || '=' || TO_CHAR(ar.NEW_TILT) ||') // ' || 'FW' ||':'||ar.SKIP_REASON 
             END  AS OSSSCRIPTCOMMANDS 
    FROM LS_CCO_MT_TILT_ACTION_REPORT AR
     WHERE AR.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID  AND AR.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP;
  /* UNION ALL
    SELECT EXECUTIONGUID,EXECUTIONSTARTTIMESTAMP,PROFILENAME,
    CATEGORY_1_ACTION,CATEGORY_2_ACTION,CATEGORY_3_ACTION,CATEGORY_4_ACTION,FINAL_ACTION,
     'CELL' AS OBJECT_TYPE,
    CELLID,CELL,
    PARAMNAME,
    CURRENT_POWER,NEW_POWER,MIN_POWER,MAX_POWER,DELTA_POWER,
    ACTION,SKIP_REASON,RETMONAME_SPLITTED,'FW' AS DIRECTION
    FROM LS_CCO_MT_POWER_ACTION_REPORT AR
     WHERE AR.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID  AND AR.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP ;
    */ 
     COMMIT;
 
 END;
 
 PROCEDURE FILL_ORIGINAL_VALUES 
 IS  
 BEGIN
 --Fill Original Values
    IF V_ROW_LS_CCO_SETTINGS.OPERATION_TYPE = 1 THEN  
    
        INSERT INTO   LS_CCO_MT_ORIGINALVALUES ( EXECUTIONGUID,EXECUTIONSTARTTIMESTAMP, EXECUTIONPLANID, OPTIMIZERID,PARAMNAME, ORIGINALVALUE, PROFILEID,RETID, RETMONAME_SPLITTED)
        SELECT /*+ FULL(AR)*/ EXECUTIONGUID, EXECUTIONSTARTTIMESTAMP, V_ROW_LS_CCO_SETTINGS.EXECUTIONPLANID,121000, PARAMNAME,CURRENT_VALUE,PROFILEID,RETID, RETMONAME_SPLITTED
        FROM LS_CCO_MT_RET_ACTION_REPORT AR
        WHERE AR.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
          AND AR.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP 
          AND NOT EXISTS (SELECT 1 FROM LS_CCO_MT_RET_ACTION_REPORT OV WHERE OV.MO=AR.MO /*AND OV.PARAMNAME=AR.PARAMNAME*/)
          AND AR.DIRECTION='FW'
          AND AR.ACTION<>'SKIP';
        
        COMMIT;
        LITESON_HELPERS.FILL_LOG_TABLE(V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP, null,'Finished to fill original values', SQL%ROWCOUNT);
        
        END IF; 
 END;
 
FUNCTION BEARINGDIFFERENCE(BEARING1 IN NUMBER, BEARING2 IN NUMBER ) 
RETURN NUMBER DETERMINISTIC PARALLEL_ENABLE IS
BEARINGDIFF NUMBER;
BEGIN 
BEARINGDIFF := ABS(BEARING1 - ROUND(BEARING2,0));
    IF  BEARINGDIFF > 180 
    THEN
        BEARINGDIFF := 360 - BEARINGDIFF;
    END IF;
RETURN BEARINGDIFF;
END; 

 PROCEDURE FILL_BORDER_LIST
 IS 
 BEGIN
  
   INSERT INTO LS_CCO_MT_BORDER_LIST (EXECUTIONSTARTTIMESTAMP,EXECUTIONGUID,SITE,LATITUDE,LONGITUDE,WEIGHT) 
          SELECT EXECUTIONSTARTTIMESTAMP,EXECUTIONGUID,NSITENAME,MIN(S_LATITUDE)LATITUDE,MIN(S_LONGITUDE) AS LONGITUDE,COUNT(*) FROM 
          (
              SELECT
              REL.EXECUTIONSTARTTIMESTAMP,REL.EXECUTIONGUID,REL.CELL,REL.LATITUDE AS S_LATITUDE,REL.LONGITUDE  AS S_LONGITUDE,
              REL.LATITUDE T_LATITUDE, REL.NSITENAME,
              REL.LONGITUDE T_LONGITUDE,REL.NCELL ,REL.HO_ATTEMPT ,REL.NCELL,
              RANK() OVER (PARTITION BY REL.CELLID ORDER BY REL.HO_ATTEMPT DESC NULLS LAST,NCELLID) NBR_RANK
              FROM LS_CCO_MT_ALL_RELS REL  
              WHERE SNAPSHOTID ='ActionPeriod' 
                AND REL.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP  
                AND REL.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
                AND REL.ARFCN=REL.NARFCN
                AND (ISTARGET=1 AND ISEXCLUDED=0) AND (NISTARGET = 0 OR NISEXCLUDED = 1) 
                AND REL.SITENAME<>REL.NSITENAME 
                AND REL.CLID = 322 
                AND REL.NCLID=322 /* TODO:GOKHAN TECH */
          ) 
          WHERE NBR_RANK<=5
          GROUP BY EXECUTIONSTARTTIMESTAMP,EXECUTIONGUID,NSITENAME; 
          
     /*********************************************************************************************/
     
      MERGE INTO LS_CCO_MT_BORDER_LIST BR
           USING
           (
               SELECT DISTINCT EXECUTIONSTARTTIMESTAMP,EXECUTIONGUID,SITE,MIN_DIST,  
                               ROUND(AVG(MIN_DIST) OVER () ,2)AVG_DIST,
                               CASE WHEN AVG(MIN_DIST) OVER ()*3<MIN_DIST THEN 1 ELSE 0 END IS_EXCLUDED
              FROM 
               (   
               SELECT BR1.EXECUTIONSTARTTIMESTAMP,BR1.EXECUTIONGUID, BR1.SITE,BR2.SITE N_SITE,
               CALCDISTANCE_JAVA(BR1.LATITUDE,BR1.LONGITUDE,BR2.LATITUDE,BR2.LONGITUDE) DIST ,
               MIN(CALCDISTANCE_JAVA(BR1.LATITUDE,BR1.LONGITUDE,BR2.LATITUDE,BR2.LONGITUDE)) OVER (PARTITION BY BR1.SITE) MIN_DIST,
               BR1.WEIGHT
               FROM LS_CCO_MT_BORDER_LIST BR1 CROSS JOIN LS_CCO_MT_BORDER_LIST BR2 
               WHERE 1=1
               AND BR1.SITE<>BR2.SITE  
               AND BR1.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
               AND BR2.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
               AND BR1.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
               AND BR2.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
               )
           )SRC
           ON( SRC.SITE=BR.SITE AND  BR.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID and BR.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP)
           WHEN MATCHED THEN UPDATE SET 
           BR.CLOSEST_SITE_DISTANCE=MIN_DIST,
           BR.AVG_DISTANCE_OF_CLUSTER=SRC.AVG_DIST,
           BR.IS_EXCLUDED=SRC.IS_EXCLUDED;  
           
           COMMIT; 
 END;
  
PROCEDURE FILL_BIN_CELL_LIST 
  IS 
    ClusterCenterLat NUMBER;
    ClusterCenterLon NUMBER;   
    queryText VARCHAR2(32000);
   v_row_settings LS_CCO_MT_GENERAL_SETTINGS%rowtype;
     v_BINSIZEINMETER number :=100;
     v_IRATCOEFFICENT number := 100; 
     v_IratCoefficient constant number :=100;                   
     v_BadQualityCoefficient constant number :=10;
BEGIN
  
    select  * into v_row_settings from     LS_CCO_MT_GENERAL_SETTINGS 
                            where   EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP  
                          AND EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID AND ROWNUM =1;


        /*************** BEGIN 2.2.3.2. Fill LS_CCO_MULTITECH_BIN_CELL_LIST "BCL" (Phase - II)     ***************************/

    INSERT /*+ APPEND */ INTO LS_CCO_MT_BIN_CELL_LIST(EXECUTIONSTARTTIMESTAMP,EXECUTIONGUID,X_INDEX,Y_INDEX,CELLID,IRAT,WEAKCOVERAGE,WEAKCOVERAGE_COST,
                                                        BADQUALITY,BADQUALITY_COST)  
   with geo_data1 as 
    (SELECT /*+ FULL(LEG) ORDERED USE_HASH(SS,LEG) */ 
                    LEG.CELLID, 
                    LEG.PLATITUDE  ,
                    LEG.PLONGITUDE  ,
                    LEG.RSRP_AVG,
                    LEG.RSRQ_AVG, 
                    LEG.RSRP_AVG,
                    LEG.RSRP_AVG, 
                    CASE WHEN 1 /*ENDREASONID TODO:GOKHAN */=2 THEN 1 ELSE 0 END IRAT, 
                    CASE WHEN  LEG.RSRP_AVG <= v_row_settings.WEAK_RSRP_TRESHOLD THEN LEG.RSRP_CNT ELSE 0 END WEAKCOVERAGE,
                    CASE WHEN  LEG.RSRP_AVG >= v_row_settings.BAD_RSRP_THRESHOLD AND LEG.RSRQ_AVG <= v_row_settings.BAD_RSRQ_THRESHOLD  THEN RSRQ_CNT ELSE 0 END BADQUALITY  
                FROM  LS_CCO_MT_CELL SS  
                    JOIN V_GENERIC_4G_TRACE_GEO LEG  
                        ON LEG.CELLID = SS.CELLID   
                    WHERE     SS.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP  
                          AND SS.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
                          AND SS.SNAPSHOTID='ActionPeriod' --AND  ISTARGET=1
                          AND LEG.DATETIME >=  V_ROW_LS_CCO_SETTINGS.ROP_START_DATE 
                          AND LEG.DATETIME < V_ROW_LS_CCO_SETTINGS.ROP_END_DATE  
                            AND  (    ( v_row_settings.WEAK_COVERAGE_SWITCH ='true' AND LEG.RSRP_AVG <= v_row_settings.WEAK_RSRP_TRESHOLD )
                                    OR ( v_row_settings.BAD_QUALITY_SWITCH='true' AND LEG.RSRP_AVG >= v_row_settings.BAD_RSRP_THRESHOLD AND LEG.RSRQ_AVG <= v_row_settings.BAD_RSRQ_THRESHOLD ) 
                                  
                                 )
        ),
      geo_data1_agg1 as 
      (
        SELECT  
              CELLID, 
              PLATITUDE,
              PLONGITUDE,
              SUM(IRAT)AS SUM_IRAT, 
              SUM(WEAKCOVERAGE) AS SUM_WEAKCOVERAGE,
              SUM(BADQUALITY) AS SUM_BADQUALITY  
        from geo_data1
         GROUP BY CELLID,PLATITUDE,PLONGITUDE 
      ),
      COORDINATE as  
      (         SELECT 
                   ROUND(CALCDISTANCE_JAVA(MAX(B.LATITUDE),MIN(LONGITUDE),MIN(LATITUDE),MIN(LONGITUDE)) /(v_BinSizeInMeter/1000),0) VERTICAL_BIN_COUNT,
                   ROUND(CALCDISTANCE_JAVA(MAX(B.LATITUDE),MIN(LONGITUDE),MAX(LATITUDE),MAX(LONGITUDE)) /(v_BinSizeInMeter/1000),0) HORIZONTAL_BIN_COUNT,
                   MIN(B.LATITUDE) SELAT,
                   MAX(B.LATITUDE) NWLAT,
                   MAX(B.LONGITUDE) SELON,
                   MIN(B.LONGITUDE) NWLON
                 FROM LS_CCO_MT_BORDER_LIST B
                 WHERE B.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP  
                   AND B.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
      )  
      select 
           V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP ,V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID,
           TRUNC (COORDINATE.VERTICAL_BIN_COUNT *   (PLATITUDE - COORDINATE.SELAT) / (COORDINATE.NWLAT - COORDINATE.SELAT)) X_INDEX,
           TRUNC (COORDINATE.HORIZONTAL_BIN_COUNT * (PLONGITUDE - COORDINATE.NWLON) / (COORDINATE.SELON - COORDINATE.NWLON)) Y_INDEX,
           GEO_DATA1_AGG1.CELLID, 
           SUM(SUM_IRAT),
           SUM(SUM_WEAKCOVERAGE),
           NVL((SUM(SUM_IRAT)*V_IRATCOEFFICENT),0) + NVL((SUM(SUM_WEAKCOVERAGE)*v_row_settings.WEAK_COVERAGE_COEFF),0) as WEAKCOVERAGE_COST,
            SUM(SUM_BADQUALITY),
           NVL((SUM(SUM_IRAT)*V_IRATCOEFFICENT),0) + NVL((SUM(SUM_BADQUALITY)*v_BadQualityCoefficient),0) as WEAKCOVERAGE_COST
       from geo_data1_agg1 CROSS JOIN COORDINATE 
        WHERE 1=1
        AND (SUM_IRAT>0 OR SUM_WEAKCOVERAGE>0 OR SUM_BADQUALITY>0)
        AND PLATITUDE IS NOT NULL
        AND PLATITUDE>COORDINATE.SELAT AND PLATITUDE<COORDINATE.NWLAT
        AND PLONGITUDE>COORDINATE.NWLON AND PLONGITUDE<COORDINATE.SELON
    GROUP BY TRUNC (COORDINATE.VERTICAL_BIN_COUNT * (PLATITUDE - COORDINATE.SELAT) / (COORDINATE.NWLAT - COORDINATE.SELAT)) ,
             TRUNC (COORDINATE.HORIZONTAL_BIN_COUNT * (PLONGITUDE - COORDINATE.NWLON) / (COORDINATE.SELON - COORDINATE.NWLON)),
           GEO_DATA1_AGG1.CELLID; 
             
    COMMIT;
    LITESON_HELPERS.FILL_LOG_TABLE(V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID,V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,-1, 
    'LS_CCO_MT_BIN_CELL_LIST Table columns filled. Affected row(s) count: '||TO_CHAR(SQL%ROWCOUNT));
       
  /*************** END 2.2.3.2. Fill LS_CCO_MULTITECH_BIN_CELL_LIST "BCL" (Phase - II)     ***************************/
  
    /**** ADDED 26.04.2020 GOKHAN DOGAN */ 
    MERGE INTO LS_CCO_mt_BIN_CELL_LIST T
    USING LS_CCO_MT_CELL S 
    ON (     T.EXECUTIONSTARTTIMESTAMP=S.EXECUTIONSTARTTIMESTAMP AND T.EXECUTIONGUID=S.EXECUTIONGUID AND T.CELLID=S.CELLID
         AND  S.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP  AND S.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
       ) 
    WHEN MATCHED THEN UPDATE SET 
    T.LATITUDE  = S.LATITUDE,
    T.LONGITUDE = S.LONGITUDE;
    
     COMMIT;

  /*************************  BEGIN  2.2.3.3. Fill LS_CCO_MULTITECH_BIN_LIST "BL" (Phase - II)  *****************************/
     --Fill initial bin list
    INSERT INTO LS_CCO_MT_BIN_LIST (EXECUTIONSTARTTIMESTAMP,EXECUTIONGUID,X_INDEX,Y_INDEX,IRAT,WEAKCOVERAGE,WEAKCOVERAGE_COST,BADQUALITY,BADQUALITY_COST)
    SELECT V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,
            V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID,
            X_INDEX,
            Y_INDEX,
            SUM(IRAT),
            SUM(WEAKCOVERAGE) ,
            NVL((sum(IRAT)*v_IRATCOEFFICENT),0) + NVL((sum(WEAKCOVERAGE)*v_row_settings.WEAK_COVERAGE_COEFF),0) as WEAKCOVERAGE_COST,
            SUM(BADQUALITY) ,
            NVL(sum(BADQUALITY)*v_BadQualityCoefficient,0)  AS BADQUALITY_COST /* TODO:GOKHAN PERC */
        FROM LS_CCO_MT_BIN_CELL_LIST BCL   
    WHERE    BCL.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
         AND BCL.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
    GROUP BY X_INDEX,Y_INDEX;

    LITESON_HELPERS.FILL_LOG_TABLE(V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,-1, 
    'LS_CCO_MT_BIN_LIST Table badcoverage_cost column filled. Affected row(s) count: '||TO_CHAR(SQL%ROWCOUNT));
     
    MERGE /*+ ORDERED */ INTO LS_CCO_MT_BIN_LIST BL 
    USING
    (  
    WITH COORDINATE AS 
            (   SELECT ROUND(CALCDISTANCE_JAVA(MAX(B.LATITUDE),MIN(LONGITUDE),MIN(LATITUDE),MIN(LONGITUDE)) /(V_BinSizeInMeter/1000),0) VERTICAL_BIN_COUNT,
                    ROUND(CALCDISTANCE_JAVA(MAX(B.LATITUDE),MIN(LONGITUDE),MAX(LATITUDE),MAX(LONGITUDE)) /(V_BinSizeInMeter/1000),0) HORIZONTAL_BIN_COUNT,
                    MIN(B.LATITUDE) SELAT,
                    MAX(B.LATITUDE) NWLAT,
                    MAX(B.LONGITUDE) SELON,
                    MIN(B.LONGITUDE) NWLON
                FROM LS_CCO_MT_BORDER_LIST B
                WHERE B.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
                  AND B.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
            ) 
        SELECT BIN.*,
            SELAT + (X_INDEX + 0.5) * ((NWLAT-SELAT)/DECODE(VERTICAL_BIN_COUNT,0,NULL,VERTICAL_BIN_COUNT)) BINCENTERLAT,
            NWLON + (Y_INDEX + 0.5) * ((SELON-NWLON)/DECODE(HORIZONTAL_BIN_COUNT,0,NULL,HORIZONTAL_BIN_COUNT)) BINCENTERLON  
        FROM LS_CCO_MT_BIN_LIST BIN 
        CROSS JOIN  COORDINATE
        WHERE  BIN.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP AND BIN.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
    )SRC
    ON(    BL.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP AND BL.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID 
       AND BL.X_INDEX=SRC.X_INDEX AND BL.Y_INDEX=SRC.Y_INDEX)
    WHEN MATCHED THEN UPDATE SET 
    BL.BIN_CENTER_LAT=SRC.BINCENTERLAT, 
    BL.BIN_CENTER_LON=SRC.BINCENTERLON 
     ;
    
    LITESON_HELPERS.FILL_LOG_TABLE(V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,-1, 
    'LS_CCO_PLUS_BIN_CELL_LIST Table BIN Coordinate columns filled. Affected row(s) count: '||TO_CHAR(SQL%ROWCOUNT));
     
      MERGE INTO LS_CCO_MT_BIN_LIST BL USING
    (
       SELECT BIN.EXECUTIONSTARTTIMESTAMP,BIN.EXECUTIONGUID,BIN.X_INDEX,BIN.Y_INDEX,
       NVL(SUM(BIN.WEAKCOVERAGE_COST),0)+NVL(SUM(NW.WEAKCOVERAGE_COST),0)+NVL(SUM(N.WEAKCOVERAGE_COST),0)+NVL(SUM(NE.WEAKCOVERAGE_COST),0)+
       NVL(SUM(E.WEAKCOVERAGE_COST),0)+NVL(SUM(SW.WEAKCOVERAGE_COST),0)+NVL(SUM(S.WEAKCOVERAGE_COST),0)+NVL(SUM(SE.WEAKCOVERAGE_COST),0)+NVL(SUM(W.WEAKCOVERAGE_COST),0) ACCUMULATED_WEAKCOV_COST,
        NVL(SUM(BIN.BADQUALITY_COST),0)+NVL(SUM(NW.BADQUALITY_COST),0)+NVL(SUM(N.BADQUALITY_COST),0)+NVL(SUM(NE.BADQUALITY_COST),0)+
       NVL(SUM(E.BADQUALITY_COST),0)+NVL(SUM(SW.BADQUALITY_COST),0)+NVL(SUM(S.BADQUALITY_COST),0)+NVL(SUM(SE.BADQUALITY_COST),0)+NVL(SUM(W.BADQUALITY_COST),0) ACCUMULATED_BADQUALITY_COST
       FROM LS_CCO_MT_BIN_LIST BIN
           LEFT JOIN LS_CCO_MT_BIN_LIST NW ON NW.EXECUTIONSTARTTIMESTAMP=BIN.EXECUTIONSTARTTIMESTAMP AND NW.EXECUTIONGUID=BIN.EXECUTIONGUID AND BIN.X_INDEX+1=NW.X_INDEX AND BIN.Y_INDEX+1=NW.Y_INDEX 
           LEFT JOIN LS_CCO_MT_BIN_LIST N  ON  N.EXECUTIONSTARTTIMESTAMP=BIN.EXECUTIONSTARTTIMESTAMP AND N.EXECUTIONGUID=BIN.EXECUTIONGUID AND BIN.X_INDEX   =N.X_INDEX AND BIN.Y_INDEX+1 =N.Y_INDEX
           LEFT JOIN LS_CCO_MT_BIN_LIST NE ON NE.EXECUTIONSTARTTIMESTAMP=BIN.EXECUTIONSTARTTIMESTAMP AND NE.EXECUTIONGUID=BIN.EXECUTIONGUID AND BIN.X_INDEX+1=NE.X_INDEX AND BIN.Y_INDEX-1=NE.Y_INDEX
           LEFT JOIN LS_CCO_MT_BIN_LIST E  ON  E.EXECUTIONSTARTTIMESTAMP=BIN.EXECUTIONSTARTTIMESTAMP AND E.EXECUTIONGUID=BIN.EXECUTIONGUID AND BIN.X_INDEX+1 =E.X_INDEX AND BIN.Y_INDEX   =E.Y_INDEX
           LEFT JOIN LS_CCO_MT_BIN_LIST SW ON SW.EXECUTIONSTARTTIMESTAMP=BIN.EXECUTIONSTARTTIMESTAMP AND SW.EXECUTIONGUID=BIN.EXECUTIONGUID AND BIN.X_INDEX-1=SW.X_INDEX AND BIN.Y_INDEX-1=SW.Y_INDEX 
           LEFT JOIN LS_CCO_MT_BIN_LIST S  ON  S.EXECUTIONSTARTTIMESTAMP=BIN.EXECUTIONSTARTTIMESTAMP AND S.EXECUTIONGUID=BIN.EXECUTIONGUID AND BIN.X_INDEX   =S.X_INDEX AND BIN.Y_INDEX-1 =S.Y_INDEX
           LEFT JOIN LS_CCO_MT_BIN_LIST SE ON SE.EXECUTIONSTARTTIMESTAMP=BIN.EXECUTIONSTARTTIMESTAMP AND SE.EXECUTIONGUID=BIN.EXECUTIONGUID AND BIN.X_INDEX-1=SE.X_INDEX AND BIN.Y_INDEX+1=SE.Y_INDEX
           LEFT JOIN LS_CCO_MT_BIN_LIST W  ON  W.EXECUTIONSTARTTIMESTAMP=BIN.EXECUTIONSTARTTIMESTAMP AND W.EXECUTIONGUID=BIN.EXECUTIONGUID AND BIN.X_INDEX-1 =W.X_INDEX AND BIN.Y_INDEX   =W.Y_INDEX
       WHERE BIN.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
         AND BIN.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
       GROUP BY BIN.EXECUTIONSTARTTIMESTAMP,BIN.EXECUTIONGUID,BIN.X_INDEX,BIN.Y_INDEX
    )
      SRC
       ON(BL.X_INDEX=SRC.X_INDEX AND BL.Y_INDEX=SRC.Y_INDEX
          AND BL.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP AND BL.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID  )
       WHEN MATCHED THEN UPDATE SET
         BL.ACCUMULATED_WEAKCOV_COST = SRC.ACCUMULATED_WEAKCOV_COST,
         BL.ACCUMULATED_BADQUALITY_COST = SRC.ACCUMULATED_BADQUALITY_COST ; 
        
        
    LITESON_HELPERS.FILL_LOG_TABLE(V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,-1, 
    'LS_CCO_MT_BIN_LIST Table ACCUMULATED_WEAKCOV_COST and ACCUMULATED_BADQUAL_COST column filled. Affected row(s) count: '||TO_CHAR(SQL%ROWCOUNT));
    
 COMMIT;
  
  END;
  
  /*********** BEGIN 2.2.3.4. Calculating WEAKCOV_WORKING_BIN_INDEX and BADQUAL_WORKING_BIN_INDEX (Phase - II) ********************/
 PROCEDURE FILL_WEAKCOV_WORKING_BINS
 IS 
 
v_RemainingBinCount number;
v_WorstBinPercValue number;
v_BinBadCoverageCost number :=0;
v_TempBadCoverageCost number;
V_ROW_SETTINGS LS_CCO_MT_GENERAL_SETTINGS%rowtype;

BEGIN
  
    select * into V_ROW_SETTINGS
     FROM LS_CCO_MT_GENERAL_SETTINGS T  
        WHERE  T.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID  
           AND T.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP 
           AND T.WEAK_COVERAGE_SWITCH='true'
           AND ROWNUM=1;

    INSERT INTO LS_CCO_MT_T_INSIDECLUSTERBINS (X_INDEX,Y_INDEX,ACCUMULATED_WEAKCOV_COST,WEAKCOVERAGE)
    SELECT BL.X_INDEX,BL.Y_INDEX,BL.ACCUMULATED_WEAKCOV_COST,BL.WEAKCOVERAGE 
    FROM LS_CCO_mt_BIN_LIST BL 
    WHERE BL.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
      AND BL.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
    ORDER BY BL.ACCUMULATED_WEAKCOV_COST DESC;
 
    SELECT 
        SUM(WEAKCOVERAGE) * (V_ROW_SETTINGS.WEAK_CELL_THRESHOLD ),
        COUNT(*)    
        INTO v_WorstBinPercValue ,v_RemainingBinCount
    FROM LS_CCO_MT_T_INSIDECLUSTERBINS ;
       
 /************************************************************************/
    LOOP
       
       MERGE /*+ USE_HASH(BIN,SRC)  */ 
       INTO LS_CCO_MT_BIN_LIST BIN USING 
       (
            SELECT /*+ NO_PARALLEL ORDERED NO_QUERY_TRANSFORMATION */  X_INDEX+c1.adj X_INDEX,Y_INDEX+c2.adj Y_INDEX FROM
            (
                SELECT /*+ dynamic_sampling(9) */ X_INDEX,Y_INDEX  
                FROM LS_CCO_MT_T_INSIDECLUSTERBINS  
                WHERE ACCUMULATED_WEAKCOV_COST= (SELECT MAX(ACCUMULATED_WEAKCOV_COST) FROM LS_CCO_MT_T_INSIDECLUSTERBINS) AND ROWNUM=1 
            )CL,
            (select 2-level as adj from dual connect by level<4)c1,
            (select 2-level as adj from dual connect by level<4)c2
            WHERE NOT EXISTS   
                (SELECT   1 FROM LS_CCO_MT_T_BLACKLISTEDBINS BL 
                        WHERE 
                                (      (CL.X_INDEX=BL.X_INDEX AND CL.Y_INDEX=BL.Y_INDEX)
                                    OR (CL.X_INDEX-1=BL.X_INDEX AND CL.Y_INDEX-1=BL.Y_INDEX)
                                    OR (CL.X_INDEX-1=BL.X_INDEX AND CL.Y_INDEX=BL.Y_INDEX)
                                    OR (CL.X_INDEX-1=BL.X_INDEX AND CL.Y_INDEX+1=BL.Y_INDEX)
                                    OR (CL.X_INDEX+1=BL.X_INDEX AND CL.Y_INDEX-1=BL.Y_INDEX)
                                    OR (CL.X_INDEX+1=BL.X_INDEX AND CL.Y_INDEX=BL.Y_INDEX)
                                    OR (CL.X_INDEX+1=BL.X_INDEX AND CL.Y_INDEX+1=BL.Y_INDEX)
                                    OR (CL.X_INDEX=BL.X_INDEX AND CL.Y_INDEX-1=BL.Y_INDEX)
                                    OR (CL.X_INDEX=BL.X_INDEX AND CL.Y_INDEX+1=BL.Y_INDEX)
                                )
                )
        )SRC
        ON(     SRC.X_INDEX=BIN.X_INDEX AND SRC.Y_INDEX=BIN.Y_INDEX 
            AND BIN.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
            AND BIN.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
          )
        WHEN MATCHED THEN UPDATE SET 
        WEAKCOV_WORKING_BIN_INDEX=v_workingBinIndex; 
       
        IF  SQL%ROWCOUNT=0 THEN
            DELETE LS_CCO_MT_T_INSIDECLUSTERBINS 
                WHERE ACCUMULATED_WEAKCOV_COST=(SELECT MAX(ACCUMULATED_WEAKCOV_COST) FROM LS_CCO_MT_T_INSIDECLUSTERBINS ) AND ROWNUM=1 ;
        END IF;
        
        SELECT /*+ FULL(BL) */ SUM(WEAKCOVERAGE) INTO v_TempBadCoverageCost 
        FROM LS_CCO_mt_BIN_LIST BL 
        WHERE BL.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
          AND BL.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
          and WEAKCOV_WORKING_BIN_INDEX=v_workingBinIndex;
        
        v_BinBadCoverageCost := v_BinBadCoverageCost + NVL(v_TempBadCoverageCost,0);
                
        INSERT INTO LS_CCO_MT_T_BLACKLISTEDBINS (X_INDEX,Y_INDEX)
        SELECT X_INDEX,Y_INDEX 
        FROM  LS_CCO_MT_BIN_LIST  BL
        WHERE BL.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
          AND BL.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
          and WEAKCOV_WORKING_BIN_INDEX=v_workingBinIndex ;
        
        DELETE FROM LS_CCO_MT_T_INSIDECLUSTERBINS WHERE (X_INDEX,Y_INDEX) IN 
        (
            SELECT /*+ FULL(BL) */ X_INDEX,Y_INDEX 
                FROM  LS_CCO_mt_BIN_LIST BL
                WHERE BL.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
                  AND BL.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
                  AND  WEAKCOV_WORKING_BIN_INDEX=v_workingBinIndex 
        );
        
        IF  SQL%ROWCOUNT>0 THEN
           v_workingBinIndex:=v_workingBinIndex+1;
        END IF;
        
        SELECT COUNT(*) INTO v_RemainingBinCount  FROM LS_CCO_MT_T_INSIDECLUSTERBINS  ;
         
        EXIT WHEN v_BinBadCoverageCost >= v_WorstBinPercValue OR v_WorstBinPercValue IS NULL OR v_RemainingBinCount=0;
   
    END LOOP; 
    /************************************************************************/
   
    MERGE /*+ FULL(BIN) */ INTO LS_CCO_MT_BIN_LIST BIN 
    USING 
    (
     SELECT /*+ FULL(BIN) */ WEAKCOV_WORKING_BIN_INDEX,AVG(BIN_CENTER_LAT) WORKING_BIN_LAT,AVG(BIN_CENTER_LON) WORKING_BIN_LON 
     FROM LS_CCO_MT_BIN_LIST BIN
     WHERE  BIN.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
        AND BIN.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
        AND WEAKCOV_WORKING_BIN_INDEX IS NOT NULL 
     GROUP BY WEAKCOV_WORKING_BIN_INDEX
    )
     SRC
     ON(     BIN.WEAKCOV_WORKING_BIN_INDEX=SRC.WEAKCOV_WORKING_BIN_INDEX
         AND BIN.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
         AND BIN.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
       )
     WHEN MATCHED THEN UPDATE SET 
     BIN.WEAKCOV_WORKING_BIN_CENTER_LAT=SRC.WORKING_BIN_LAT , 
     BIN.WEAKCOV_WORKING_BIN_CENTER_LON=SRC.WORKING_BIN_LON
     ;
        
    COMMIT;
END;
  /*********** END 2.2.3.4. Calculating WEAKCOV_WORKING_BIN_INDEX and BADQUAL_WORKING_BIN_INDEX (Phase - II) *********************/

 PROCEDURE FILL_BAD_QUAL_WORKING_BINS
 IS 
v_RemainingBinCount number;
v_WorstBinPercValue number;
v_BinBadCoverageCost number :=0;
v_TempBadCoverageCost number;
V_ROW_SETTINGS LS_CCO_MT_GENERAL_SETTINGS%rowtype;

BEGIN 

    select * into V_ROW_SETTINGS
     FROM LS_CCO_MT_GENERAL_SETTINGS T  
        WHERE BAD_QUALITY_SWITCH='true' and T.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID  
        AND T.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP AND ROWNUM=1;

    INSERT INTO LS_CCO_MT_T_INSIDECLUSTERBINS (X_INDEX,Y_INDEX,ACCUMULATED_BADQUALITY_COST,BADQUALITY)
    SELECT BL.X_INDEX,BL.Y_INDEX,BL.ACCUMULATED_BADQUALITY_COST,BL.BADQUALITY 
    FROM LS_CCO_mt_BIN_LIST BL 
    WHERE BL.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
      AND BL.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
    ORDER BY BL.ACCUMULATED_BADQUALITY_COST DESC;
 
    SELECT 
        SUM(BADQUALITY) * V_ROW_SETTINGS.BAD_QUALITY_PERCENTE,
        COUNT(*)    
        INTO v_WorstBinPercValue ,v_RemainingBinCount
    FROM LS_CCO_MT_T_INSIDECLUSTERBINS ;
       
 /************************************************************************/
 
    LOOP
       
       MERGE /*+ USE_HASH(BIN,SRC)  */ 
       INTO LS_CCO_MT_BIN_LIST BIN USING 
       (
            SELECT /*+ NO_PARALLEL ORDERED NO_QUERY_TRANSFORMATION */  X_INDEX+c1.adj X_INDEX,Y_INDEX+c2.adj Y_INDEX FROM
            (
                SELECT /*+ dynamic_sampling(9) */ X_INDEX,Y_INDEX  
                FROM LS_CCO_MT_T_INSIDECLUSTERBINS  
                WHERE ACCUMULATED_BADQUALITY_COST= (SELECT MAX(ACCUMULATED_BADQUALITY_COST) FROM LS_CCO_MT_T_INSIDECLUSTERBINS)
                AND ROWNUM=1 
            )CL,
            (select 2-level as adj from dual connect by level<4)c1,
            (select 2-level as adj from dual connect by level<4)c2
            WHERE NOT EXISTS   
                (SELECT   1 FROM LS_CCO_MT_T_BLACKLISTEDBINS BL 
                        WHERE 
                                (      (CL.X_INDEX=BL.X_INDEX AND CL.Y_INDEX=BL.Y_INDEX)
                                    OR (CL.X_INDEX-1=BL.X_INDEX AND CL.Y_INDEX-1=BL.Y_INDEX)
                                    OR (CL.X_INDEX-1=BL.X_INDEX AND CL.Y_INDEX=BL.Y_INDEX)
                                    OR (CL.X_INDEX-1=BL.X_INDEX AND CL.Y_INDEX+1=BL.Y_INDEX)
                                    OR (CL.X_INDEX+1=BL.X_INDEX AND CL.Y_INDEX-1=BL.Y_INDEX)
                                    OR (CL.X_INDEX+1=BL.X_INDEX AND CL.Y_INDEX=BL.Y_INDEX)
                                    OR (CL.X_INDEX+1=BL.X_INDEX AND CL.Y_INDEX+1=BL.Y_INDEX)
                                    OR (CL.X_INDEX=BL.X_INDEX AND CL.Y_INDEX-1=BL.Y_INDEX)
                                    OR (CL.X_INDEX=BL.X_INDEX AND CL.Y_INDEX+1=BL.Y_INDEX)
                                )
                )
        )SRC
        ON(     SRC.X_INDEX=BIN.X_INDEX AND SRC.Y_INDEX=BIN.Y_INDEX 
            AND BIN.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
            AND BIN.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
          )
        WHEN MATCHED THEN UPDATE SET 
        BAD_QUAL_WORKING_BIN_INDEX=v_workingBinIndex; 
       
        IF  SQL%ROWCOUNT=0 THEN
            DELETE LS_CCO_MT_T_INSIDECLUSTERBINS 
                WHERE ACCUMULATED_BADQUALITY_COST=(SELECT MAX(ACCUMULATED_BADQUALITY_COST) FROM LS_CCO_MT_T_INSIDECLUSTERBINS ) AND ROWNUM=1
                  ;
        END IF;
        
        SELECT /*+ FULL(BL) */ SUM(BADQUALITY) INTO v_TempBadCoverageCost 
        FROM LS_CCO_mt_BIN_LIST BL 
        WHERE BL.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
          AND BL.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
          and BAD_QUAL_WORKING_BIN_INDEX=v_workingBinIndex;
        
        v_BinBadCoverageCost := v_BinBadCoverageCost + NVL(v_TempBadCoverageCost,0);
                
        INSERT INTO LS_CCO_MT_T_BLACKLISTEDBINS (X_INDEX,Y_INDEX)
        SELECT X_INDEX,Y_INDEX 
        FROM  LS_CCO_MT_BIN_LIST  BL
        WHERE BL.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
          AND BL.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
          and BAD_QUAL_WORKING_BIN_INDEX=v_workingBinIndex ;
        
        DELETE FROM LS_CCO_MT_T_INSIDECLUSTERBINS WHERE (X_INDEX,Y_INDEX) IN 
        (
            SELECT /*+ FULL(BL) */ X_INDEX,Y_INDEX 
                FROM  LS_CCO_mt_BIN_LIST BL
                WHERE BL.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
                  AND BL.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
                  AND  BAD_QUAL_WORKING_BIN_INDEX=v_workingBinIndex 
        );
        
        IF  SQL%ROWCOUNT>0 THEN
           v_workingBinIndex:=v_workingBinIndex+1;
        END IF;
        
        SELECT COUNT(*) INTO v_RemainingBinCount  FROM LS_CCO_MT_T_INSIDECLUSTERBINS  ;
         
        EXIT WHEN v_BinBadCoverageCost >= v_WorstBinPercValue OR v_WorstBinPercValue IS NULL OR v_RemainingBinCount=0;
   
    END LOOP; 
    /************************************************************************/
   
    MERGE /*+ FULL(BIN) */ INTO LS_CCO_MT_BIN_LIST BIN 
    USING 
    (
     SELECT /*+ FULL(BIN) */ BAD_QUAL_WORKING_BIN_INDEX,AVG(BIN_CENTER_LAT) WORKING_BIN_LAT,AVG(BIN_CENTER_LON) WORKING_BIN_LON 
     FROM LS_CCO_MT_BIN_LIST BIN
     WHERE  BIN.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
        AND BIN.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
        AND BAD_QUAL_WORKING_BIN_INDEX IS NOT NULL 
     GROUP BY BAD_QUAL_WORKING_BIN_INDEX
    )
     SRC
     ON(     BIN.BAD_QUAL_WORKING_BIN_INDEX=SRC.BAD_QUAL_WORKING_BIN_INDEX
         AND BIN.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
         AND BIN.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
       )
     WHEN MATCHED THEN UPDATE SET 
     BIN.BAD_QUAL_WORKING_BIN_CENTER_LAT = SRC.WORKING_BIN_LAT , 
     BIN.BAD_QUAL_WORKING_BIN_CENTER_LON = SRC.WORKING_BIN_LON
     ;
        
    COMMIT;
END;

/****************** BEGIN 2.2.3.5. Filling LS_CCO_MULTITECH_WEAKCOV_CANDIDATE (Phase - II) **********************/
 PROCEDURE FILL_WEAKCOV_CANDIDATE_LIST  
IS
BEGIN 
     
        INSERT /*+ APPEND */ INTO LS_CCO_MT_WEAKCOV_CANDIDATE (EXECUTIONSTARTTIMESTAMP,EXECUTIONGUID,X_INDEX,Y_INDEX,CELLID,UE_LOST,IRAT,WEAKCOVERAGE,WEAKCOVERAGE_COST,
                                                               WORKINGBIN_CELL_WEAKCOV_COST)    
        SELECT /*+ ORDERED FULL(BIN) FULL(CELL) USE_HASH(BIN,CELL) */ CELL.EXECUTIONSTARTTIMESTAMP,
               CELL.EXECUTIONGUID,
               CELL.X_INDEX,
               CELL.Y_INDEX,
               CELL.CELLID,
               CELL.UE_LOST,
               CELL.IRAT,
               CELL.WEAKCOVERAGE,
               CELL.WEAKCOVERAGE_COST,
               SUM(CELL.WEAKCOVERAGE_COST) OVER (PARTITION BY WEAKCOV_WORKING_BIN_INDEX,CELL.CELLID) WORKINGBIN_CELL_WEAKCOV_COST
        FROM LS_CCO_MT_BIN_LIST BIN 
        JOIN LS_CCO_mt_BIN_CELL_LIST CELL 
               ON  BIN.EXECUTIONSTARTTIMESTAMP=CELL.EXECUTIONSTARTTIMESTAMP 
               AND BIN.EXECUTIONGUID=CELL.EXECUTIONGUID 
               AND BIN.X_INDEX=CELL.X_INDEX AND BIN.Y_INDEX=CELL.Y_INDEX
         WHERE BIN.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
           AND BIN.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
            AND BIN.WEAKCOV_WORKING_BIN_INDEX IS NOT NULL
          ;
    COMMIT;
 /*******************************************************************************************************************************/ 
  
     MERGE INTO LS_CCO_MT_BIN_LIST BIN
         USING
         (
            SELECT /*+ USE_HASH(BIN CELL) USE_HASH(SS CELL) ORDERED */ 
                WEAKCOV_WORKING_BIN_INDEX , 
                ROUND(AVG(CALCDISTANCE_JAVA(SS.LATITUDE,SS.LONGITUDE,BIN.WEAKCOV_WORKING_BIN_CENTER_LAT,BIN.WEAKCOV_WORKING_BIN_CENTER_LON)),2) + 
                (3 * ROUND(STDDEV(CALCDISTANCE_JAVA(SS.LATITUDE,SS.LONGITUDE,BIN.WEAKCOV_WORKING_BIN_CENTER_LAT,BIN.WEAKCOV_WORKING_BIN_CENTER_LON)),2) ) AS MAX_DISTANCE
            FROM LS_CCO_MT_BIN_LIST BIN 
            JOIN LS_CCO_MT_BIN_CELL_LIST CELL 
                ON BIN.EXECUTIONSTARTTIMESTAMP=CELL.EXECUTIONSTARTTIMESTAMP AND BIN.EXECUTIONGUID=CELL.EXECUTIONGUID 
                AND BIN.X_INDEX=CELL.X_INDEX AND BIN.Y_INDEX=CELL.Y_INDEX AND BIN.WEAKCOV_WORKING_BIN_INDEX IS NOT NULL
            JOIN LS_CCO_MT_CELL_ALL_KPIS SS 
                ON SS.EXECUTIONSTARTTIMESTAMP=CELL.EXECUTIONSTARTTIMESTAMP AND SS.EXECUTIONGUID=CELL.EXECUTIONGUID AND SS.CELLID=CELL.CELLID
            WHERE BIN.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP AND BIN.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
              AND SS.SNAPSHOTID='ActionPeriod'  
            GROUP BY WEAKCOV_WORKING_BIN_INDEX 
         ) SRC
         ON(BIN.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP AND BIN.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID 
            AND BIN.WEAKCOV_WORKING_BIN_INDEX=SRC.WEAKCOV_WORKING_BIN_INDEX 
           )
         WHEN MATCHED THEN UPDATE SET 
         BIN.WEAKCOV_MAX_DISTANCE=SRC.MAX_DISTANCE 
         ; 
        COMMIT;
    /*******************************************************************************************************************************/

        MERGE INTO LS_CCO_MT_WEAKCOV_CANDIDATE CC 
        USING(
               WITH V_ROW_SETTINGS  AS 
                   (    select S.*, 
                      nvl(WEAK_RELATIVE_AZIMUTH_COEFF,0) + 
                       NVL(WEAK_HEIGHT_COEFF,0) + 
                       NVL(WEAK_TILT_COEFF,0) + 
                       NVL(WEAK_COVERAGE_COEFF,0) + 
                       NVL(WEAK_DISTANCE_COEFF,0) +
                       NVL(WEAK_PRBUTILIZATION_COEFF,0) AS coefficientSum  
                       from LS_CCO_MT_GENERAL_SETTINGS S  
                       where V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP = s.EXECUTIONSTARTTIMESTAMP 
                       AND  V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP = s.EXECUTIONSTARTTIMESTAMP  
                       and CLID=322 AND WEAK_COVERAGE_SWITCH='true'  AND ROWNUM =1
                   ),
              V_AVG_DIST_VALUES AS
                   (
                      select   CELLID,     ROUND(AVG(AVG_DIST_BEST2SITE),3) AS AVG_DIST_BEST2SITE  ,AVG(TAPC90_DIST) AS TAPC90_DIST
                      from LS_CCO_MT_ALL_REPORT S
                      WHERE AVG_DIST_BEST2SITE IS NOT NULL AND PROCESS_TYPE='UNDERSHOOT'  
                       AND  V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP = s.EXECUTIONSTARTTIMESTAMP 
                       AND  V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP = s.EXECUTIONSTARTTIMESTAMP  
                      GROUP BY CELLID
                   )  
              SELECT /*+ ORDERED USE_HASH(CELL,BIN) USE_HASH(CELL,SS,LC4C,KPI) */ 
              CELL.X_INDEX,CELL.Y_INDEX,CELL.CELLID, 
                ROUND(CASE WHEN LS_CCO_MT.BEARINGDIFFERENCE(ROUND(LC4C.AZIMUTH),  ROUND(MOD( 360 + GEO$TO_DEGREE(BEARING_2POINTS(LC4C.LATITUDE, LC4C.LONGITUDE, BIN.WEAKCOV_WORKING_BIN_CENTER_LAT, WEAKCOV_WORKING_BIN_CENTER_LON)) , 360))  ) > V_ROW_SETTINGS.WEAK_RELATIVE_AZIMUTH_COEFF THEN 0 
                           ELSE  1-(((LS_CCO_MT.BEARINGDIFFERENCE(ROUND(LC4C.AZIMUTH), ROUND(MOD( 360 + GEO$TO_DEGREE(BEARING_2POINTS(LC4C.LATITUDE, LC4C.LONGITUDE, BIN.WEAKCOV_WORKING_BIN_CENTER_LAT, WEAKCOV_WORKING_BIN_CENTER_LON)) , 360))  )/V_ROW_SETTINGS.WEAK_RELATIVE_AZIMUTH_COEFF))*(V_ROW_SETTINGS.WEAK_RELATIVE_AZIMUTH_COEFF/coefficientSum))
                      END ,6
                     ) NormRelativeAzimuth,  
                ------------------
                CASE WHEN LS_CCO_MT.BEARINGDIFFERENCE(ROUND(LC4C.AZIMUTH),  ROUND(MOD( 360 + GEO$TO_DEGREE(BEARING_2POINTS(LC4C.LATITUDE, LC4C.LONGITUDE, BIN.WEAKCOV_WORKING_BIN_CENTER_LAT, WEAKCOV_WORKING_BIN_CENTER_LON)) , 360))  ) > V_ROW_SETTINGS.WEAK_RELATIVE_AZIMUTH_COEFF THEN 1 ELSE 0 END AS RelativeAzimuthConstraint, 
                --------------------
               ROUND(
                       CASE WHEN TO_NUMBER(LC4C.HEIGHT)>V_ROW_SETTINGS.MaxHeightThd THEN 0 
                        ELSE (TO_NUMBER(LC4C.HEIGHT)-(MIN(TO_NUMBER(LC4C.HEIGHT)) OVER (PARTITION BY WEAKCOV_WORKING_BIN_INDEX))) / 
                        DECODE( (V_ROW_SETTINGS.MaxHeightThd-(MIN(TO_NUMBER(LC4C.HEIGHT)) OVER (PARTITION BY WEAKCOV_WORKING_BIN_INDEX))) , 0 , NULL,
                                (V_ROW_SETTINGS.MaxHeightThd-(MIN(TO_NUMBER(LC4C.HEIGHT)) OVER (PARTITION BY WEAKCOV_WORKING_BIN_INDEX)))
                              )
                         * (V_ROW_SETTINGS.WEAK_HEIGHT_COEFF/coefficientSum) END  
                     , 6 
                    ) NormHeight,
                ------------------ 
                CASE WHEN TO_NUMBER(LC4C.HEIGHT)>V_ROW_SETTINGS.WEAK_HEIGHT_COEFF THEN 1 ELSE 0 END AS HeightLimitConstraint, 
                ------------------
                ROUND
                (   (
                      (WORKINGBIN_CELL_WEAKCOV_COST - MIN(CELL.WORKINGBIN_CELL_WEAKCOV_COST) OVER (PARTITION BY WEAKCOV_WORKING_BIN_INDEX)) /
                      DECODE
                      (
                        MAX(CELL.WORKINGBIN_CELL_WEAKCOV_COST) OVER (PARTITION BY WEAKCOV_WORKING_BIN_INDEX) - MIN(CELL.WORKINGBIN_CELL_WEAKCOV_COST) OVER (PARTITION BY WEAKCOV_WORKING_BIN_INDEX)
                        ,0,NULL,
                        MAX(CELL.WORKINGBIN_CELL_WEAKCOV_COST) OVER (PARTITION BY WEAKCOV_WORKING_BIN_INDEX) - MIN(CELL.WORKINGBIN_CELL_WEAKCOV_COST) OVER (PARTITION BY WEAKCOV_WORKING_BIN_INDEX)
                      )
                    ) * 
                    (V_ROW_SETTINGS.WEAK_COVERAGE_COEFF / coefficientSum)
                , 6
                ) AS NormBadCoverageCost,
               ------------------
                ROUND(CASE WHEN TO_NUMBER(LC4C.TILT) <  V_ROW_SETTINGS.TILT_MIN_UI THEN 0
                           WHEN TO_NUMBER(LC4C.TILT) >  V_ROW_SETTINGS.TILT_MAX_UI THEN 1 * (V_ROW_SETTINGS.WEAK_TILT_COEFF/coefficientSum)
                           ELSE (TO_NUMBER(LC4C.TILT) - V_ROW_SETTINGS.TILT_MIN_UI) / (V_ROW_SETTINGS.TILT_MAX_UI -    V_ROW_SETTINGS.TILT_MIN_UI)  * (V_ROW_SETTINGS.WEAK_TILT_COEFF/coefficientSum)
                      END
                    , 6 
                     ) AS NormTilt, 
                ROUND(CASE WHEN CALCDISTANCE_JAVA(LC4C.LATITUDE, LC4C.LONGITUDE, BIN.WEAKCOV_WORKING_BIN_CENTER_LAT, WEAKCOV_WORKING_BIN_CENTER_LON) > BIN.WEAKCOV_MAX_DISTANCE OR 
                                CALCDISTANCE_JAVA(LC4C.LATITUDE, LC4C.LONGITUDE, BIN.WEAKCOV_WORKING_BIN_CENTER_LAT, BIN.WEAKCOV_WORKING_BIN_CENTER_LON) < 0.25 THEN 0
                           ELSE  (1 - (CALCDISTANCE_JAVA(LC4C.LATITUDE, LC4C.LONGITUDE, BIN.WEAKCOV_WORKING_BIN_CENTER_LAT, WEAKCOV_WORKING_BIN_CENTER_LON)/BIN.WEAKCOV_MAX_DISTANCE)) * (V_ROW_SETTINGS.WEAK_DISTANCE_COEFF/coefficientSum) 
                      END
                    , 6 
                     ) AS  NormDist, 
                ROUND(CASE WHEN LC4C.PRB_UTILIZATION IS NULL OR LC4C.PRB_UTILIZATION > V_ROW_SETTINGS.MAXIMUM_RESOURCE_UTILIZATION THEN 0
                           ELSE (1- (LC4C.PRB_UTILIZATION/V_ROW_SETTINGS.MAXIMUM_RESOURCE_UTILIZATION)) * (V_ROW_SETTINGS.WEAK_PRBUTILIZATION_COEFF/coefficientSum) 
                      END    
                    , 6  ) AS  NormPrbUtilization, 
                ------------------
                CASE WHEN (LC4C.PRB_UTILIZATION IS NULL OR  LC4C.PRB_UTILIZATION > V_ROW_SETTINGS.MAXIMUM_RESOURCE_UTILIZATION) THEN 1 ELSE 0 END AS PrbUtilizationConstraint, 
                ------------------ 
                  CASE WHEN V_AVG_DIST_VALUES.AVG_DIST_BEST2SITE is   null OR V_AVG_DIST_VALUES.TAPC90_DIST > V_AVG_DIST_VALUES.AVG_DIST_BEST2SITE THEN 1 ELSE 0 END  AS TACONSTRAINT
                FROM 
                LS_CCO_MT_WEAKCOV_CANDIDATE CELL
                JOIN LS_CCO_MT_BIN_LIST BIN 
                 ON  BIN.EXECUTIONSTARTTIMESTAMP=CELL.EXECUTIONSTARTTIMESTAMP AND CELL.EXECUTIONGUID=BIN.EXECUTIONGUID 
                 AND CELL.X_INDEX=BIN.X_INDEX AND CELL.Y_INDEX=BIN.Y_INDEX   
                JOIN LS_CCO_MT_CELL_ALL_KPIS LC4C 
                 ON LC4C.EXECUTIONSTARTTIMESTAMP=CELL.EXECUTIONSTARTTIMESTAMP AND LC4C.EXECUTIONGUID=CELL.EXECUTIONGUID 
                 AND LC4C.CELLID=CELL.CELLID
                 LEFT JOIN V_AVG_DIST_VALUES ON (V_AVG_DIST_VALUES.CELLID=LC4C.CELLID)
                cross join   V_ROW_SETTINGS  
              WHERE     
                         CELL.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
                     AND CELL.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
                     AND WEAKCOV_WORKING_BIN_INDEX IS NOT NULL
                     AND LC4C.SNAPSHOTID='ActionPeriod'  
        )SRC
        ON(     SRC.X_INDEX=CC.X_INDEX AND SRC.Y_INDEX=CC.Y_INDEX AND SRC.CELLID=CC.CELLID 
            AND CC.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP 
            AND CC.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID)
        WHEN MATCHED THEN UPDATE SET CC.NORMRELATIVEAZIMUTH = SRC.NORMRELATIVEAZIMUTH ,
                                     CC.NORMHEIGHT = SRC.NORMHEIGHT, 
                                     CC.NORMBADCOVERAGECOST = SRC.NORMBADCOVERAGECOST ,
                                     CC.NORMTILT = SRC.NORMTILT ,
                                     CC.NORMDIST = SRC.NORMDIST ,
                                     CC.NORMPRBUTILIZATION = SRC.NORMPRBUTILIZATION,
                                     CC.HEIGHTLIMITCONSTRAINT = SRC.HEIGHTLIMITCONSTRAINT,
                                     CC.RELATIVEAZIMUTHCONSTRAINT = SRC.RELATIVEAZIMUTHCONSTRAINT,
                                     CC.PRBUTILIZATIONCONSTRAINT = SRC.PRBUTILIZATIONCONSTRAINT,
                                     CC.TACONSTRAINT = SRC.TACONSTRAINT
                                    /* CC.WEAKCOVBINAVGTIER1COST = SRC.WEAKCOVBINAVGTIER1COST, 
                                     CC.MEASCONSTRAINT = SRC.MEASCONSTRAINT 
                                     CC.VBWCONSTRAINT = SRC.VBWCONSTRAINT*/
                                      ;
                                     
        LITESON_HELPERS.FILL_LOG_TABLE(V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,-1, 
        'LS_CCO_MT_WEAKCOV_CANDIDATE Table Normalized columns updated. Affected row(s) count: '||TO_CHAR(SQL%ROWCOUNT));
   /*******************************************************************************************************************************/      
       MERGE INTO LS_CCO_MT_WEAKCOV_CANDIDATE CC 
        USING
        ( 
            SELECT /*+ ORDERED USE_HASH(C,KPI) */ c.EXECUTIONSTARTTIMESTAMP,c.EXECUTIONGUID, X_INDEX,Y_INDEX,CELLID,
             CASE WHEN HEIGHTLIMITCONSTRAINT=1 OR RELATIVEAZIMUTHCONSTRAINT=1 OR PRBUTILIZATIONCONSTRAINT=1 OR TACONSTRAINT=1  THEN 1 ELSE 0 END EXDLUDED,
                ROUND( NVL(NormRelativeAzimuth,0) + NVL(NormHeight,0) + NVL(NormBadCoverageCost,0) + NVL(NormTilt,0) + NVL(NormDist,0) + NVL(NormPrbUtilization,0),6) CandidateCellCost
            FROM LS_CCO_MT_WEAKCOV_CANDIDATE C  
            WHERE   C.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP 
                AND C.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID  
        )SRC
        ON(     SRC.X_INDEX=CC.X_INDEX AND SRC.Y_INDEX=CC.Y_INDEX 
            AND SRC.CELLID=CC.CELLID 
            AND CC.EXECUTIONGUID=src.EXECUTIONGUID and cc.EXECUTIONSTARTTIMESTAMP=src.EXECUTIONSTARTTIMESTAMP 
          )
        WHEN MATCHED THEN UPDATE SET 
        CC.CANDIDATECELLCOST=SRC.CANDIDATECELLCOST,
        CC.EXDLUDED=SRC.EXDLUDED,
        CC.REASON=CASE SRC.EXDLUDED WHEN 1 THEN 'Eleminated Due to Constains' ELSE NULL END ;
         COMMIT; 
        LITESON_HELPERS.FILL_LOG_TABLE(V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID, V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,-1, 'LS_CCO_MT_WEAKCOV_CANDIDATE Table CANDIDATECELLCOST column updated. Affected row(s) count: '||TO_CHAR(SQL%ROWCOUNT));
       
   /*******************************************************************************************************************************/       
   
   MERGE INTO LS_CCO_MT_CELL T
        USING
         (
           WITH V_QUERY1 AS 
               ( SELECT /*+  ORDERED USE_HASH(C,BIN) MATERIALIZE */  
                     C.CELLID,WEAKCOV_WORKING_BIN_CENTER_LAT,WEAKCOV_WORKING_BIN_CENTER_LON, WEAKCOV_WORKING_BIN_INDEX,CANDIDATECELLCOST,C.WEAKCOVERAGE_COST
                    FROM LS_CCO_MT_WEAKCOV_CANDIDATE C
                    JOIN LS_CCO_MT_BIN_LIST BIN
                    ON      C.EXECUTIONSTARTTIMESTAMP=BIN.EXECUTIONSTARTTIMESTAMP
                        AND C.EXECUTIONGUID=BIN.EXECUTIONGUID
                        AND C.X_INDEX=BIN.X_INDEX
                        AND C.Y_INDEX=BIN.Y_INDEX 
                        AND C.EXDLUDED=0
                    WHERE   C.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
                        AND C.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID  
                        AND C.CANDIDATECELLCOST>0 
                ),
               V_HIGH AS 
                ( 
                SELECT   WEAKCOV_WORKING_BIN_INDEX,MAX(CANDIDATECELLCOST) AS MAX_CANDIDATECELLCOST
                    FROM  V_QUERY1  
                    GROUP BY WEAKCOV_WORKING_BIN_INDEX
                    HAVING MAX(CANDIDATECELLCOST) > 0
                ),
               V_RESULT1 AS 
                (          
                    SELECT  
                       C.CELLID, WEAKCOV_WORKING_BIN_INDEX, WEAKCOV_WORKING_BIN_CENTER_LAT,WEAKCOV_WORKING_BIN_CENTER_LON,CANDIDATECELLCOST,SUM(C.WEAKCOVERAGE_COST) WEAKCOVERAGE_COST
                       FROM V_QUERY1 C
                       WHERE (C.WEAKCOV_WORKING_BIN_INDEX,C.CANDIDATECELLCOST) 
                                                                    IN (SELECT /*+ NO_QUERY_TRANSFORMATION */ V_HIGH.WEAKCOV_WORKING_BIN_INDEX, MAX_CANDIDATECELLCOST FROM V_HIGH )
                   GROUP BY C.CELLID, WEAKCOV_WORKING_BIN_INDEX, WEAKCOV_WORKING_BIN_CENTER_LAT, WEAKCOV_WORKING_BIN_CENTER_LON, CANDIDATECELLCOST      
                )    
           SELECT /*+ ORDERED USE_HASH(C,BIN) */ 
                     CELLID,
                     SUM(CANDIDATECELLCOST) CANDIDATECELLCOST,
                     SUM(WEAKCOVERAGE_COST) WEAKCOVERAGE_COST,
                     LISTAGG('Problem Area - Bin index: '||WEAKCOV_WORKING_BIN_INDEX ||' Bin Location:'|| ROUND(WEAKCOV_WORKING_BIN_CENTER_LAT,6) ||' , '||ROUND(WEAKCOV_WORKING_BIN_CENTER_LON,6)||' Cost:'||CANDIDATECELLCOST,' ; ')
                   WITHIN GROUP (ORDER BY CELLID)  REASON
                   FROM V_RESULT1
                   GROUP BY CELLID  
         ) map
        on (T.CELLID= MAP.CELLID AND  T.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID  AND T.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP )
         WHEN MATCHED THEN UPDATE SET
         T.ISWEAKCOVERAGE=1   ,
         T.CANDIDATECELLCOST=MAP.CANDIDATECELLCOST,
         T.WEAKCOVERAGE_COST=MAP.WEAKCOVERAGE_COST,
         T.REASON_WEAKCOVERAGE= substr(MAP.REASON,1,800)  ;
         
         COMMIT; 
           
END;


PROCEDURE FILL_BADQUAL_CANDIDATE_LIST
IS 
 v_BINSIZEINMETER number :=100;
    V_ROW_SETTINGS LS_CCO_MT_GENERAL_SETTINGS%rowtype;
BEGIN
 
    select * into V_ROW_SETTINGS
     FROM LS_CCO_MT_GENERAL_SETTINGS T  
        WHERE   T.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID  
        AND T.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP AND ROWNUM=1;

      MERGE INTO LS_CCO_MT_BIN_LIST BIN
         USING
         (
            SELECT /*+ USE_HASH(BIN CELL)     */ 
                BAD_QUAL_WORKING_BIN_INDEX , 
                ROUND(AVG(CALCDISTANCE_JAVA(CELL.LATITUDE,CELL.LONGITUDE,BIN.BAD_QUAL_WORKING_BIN_CENTER_LAT,BIN.BAD_QUAL_WORKING_BIN_CENTER_LON)),2) + 
                (3 * ROUND(STDDEV(CALCDISTANCE_JAVA(CELL.LATITUDE,CELL.LONGITUDE,BIN.BAD_QUAL_WORKING_BIN_CENTER_LAT,BIN.BAD_QUAL_WORKING_BIN_CENTER_LON)),2) ) AS MAX_DISTANCE
            FROM LS_CCO_MT_BIN_LIST BIN 
            JOIN LS_CCO_MT_BIN_CELL_LIST CELL 
                ON BIN.EXECUTIONSTARTTIMESTAMP=CELL.EXECUTIONSTARTTIMESTAMP AND BIN.EXECUTIONGUID=CELL.EXECUTIONGUID 
                AND BIN.X_INDEX=CELL.X_INDEX AND BIN.Y_INDEX=CELL.Y_INDEX AND BIN.BAD_QUAL_WORKING_BIN_INDEX IS NOT NULL 
            WHERE BIN.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP AND BIN.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP  
            GROUP BY BAD_QUAL_WORKING_BIN_INDEX 
         ) SRC
         ON(BIN.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP AND BIN.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID 
            AND BIN.BAD_QUAL_WORKING_BIN_INDEX=SRC.BAD_QUAL_WORKING_BIN_INDEX 
           )
         WHEN MATCHED THEN UPDATE SET 
         BIN.BAD_QUAL_MAX_DISTANCE=SRC.MAX_DISTANCE ; 
         
    /********************************************************************************/     

 INSERT /*+ APPEND */ INTO LS_CCO_MT_BAD_QUAL_CANDIDATE 
                    (EXECUTIONSTARTTIMESTAMP,EXECUTIONGUID,PROFILEID, CLID,  X_INDEX,Y_INDEX,CELLID,LATITUDE,LONGITUDE,BADQUALITY,BADQUALITY_COST ,
                      BAD_QUAL_WORKING_BIN_INDEX,  BAD_QUAL_WORKING_BIN_CENTER_LAT, BAD_QUAL_WORKING_BIN_CENTER_LON,  BAD_QUAL_MAX_DISTANCE
                    )                          
        SELECT /*+ ORDERED FULL(BIN) FULL(CELL) USE_HASH(BIN,CELL) */  
               CELL.EXECUTIONSTARTTIMESTAMP,
               CELL.EXECUTIONGUID,
               CELL.PROFILEID,
               CELL.CLID,
               CELL.X_INDEX,  
               CELL.Y_INDEX, 
               CELL.CELLID,
               CELL.LATITUDE,
               CELL.LONGITUDE,
               CELL.BADQUALITY,
               CELL.BADQUALITY_COST,
               BAD_QUAL_WORKING_BIN_INDEX,     -- OK
               BAD_QUAL_WORKING_BIN_CENTER_LAT, -- OK
               BAD_QUAL_WORKING_BIN_CENTER_LON, -- OK
               BAD_QUAL_MAX_DISTANCE            -- NOK
        FROM LS_CCO_MT_BIN_LIST BIN 
        JOIN LS_CCO_mt_BIN_CELL_LIST CELL 
               ON  BIN.EXECUTIONSTARTTIMESTAMP=CELL.EXECUTIONSTARTTIMESTAMP 
               AND BIN.EXECUTIONGUID=CELL.EXECUTIONGUID 
               AND BIN.X_INDEX=CELL.X_INDEX AND BIN.Y_INDEX=CELL.Y_INDEX
         WHERE  BIN.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID
           AND BIN.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP
            AND   BIN.BAD_QUAL_WORKING_BIN_INDEX IS NOT NULL; 
         
         commit;
   /***************************************************************************************/
            
    INSERT /*+ APPEND */ INTO LS_CCO_MT_BIN_REL_LIST(EXECUTIONSTARTTIMESTAMP,EXECUTIONGUID,X_INDEX,Y_INDEX,CELLID,NCELLID,SUM_OF_POLLUTERCNT,
                                                        SERVING_TOTAL_COUNT,NCELL_TOTAL_POLLUTER_COUNT,BAD_QUAL_WORKING_BIN_INDEX,SOURCE_RANK,NEI_RANK
                                                     ) 
 with geo_data1 as 
    ( 
    SELECT /*+ FULL(LEG) ORDERED USE_HASH(SS,LEG) */ 
                    LEG.CELLID, 
                    LEG.NCELLID, 
                    LEG.PLATITUDE, 
                    LEG.PLONGITUDE,   
                    LEG.POLLUTER_CNT, 
                    LEG.BIN_INTRA_NBR_CNT,   
                    BAD_QUAL_WORKING_BIN_INDEX
                FROM  LS_CCO_MT_BAD_QUAL_CANDIDATE SS   /* todo:enis */
                    JOIN EXPERIA_POLLUTION LEG  
                        ON LEG.CELLID = SS.CELLID  
                    WHERE     SS.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP  
                          AND SS.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID 
                          AND LEG.DATETIME >=  V_ROW_LS_CCO_SETTINGS.ROP_START_DATE
                            AND LEG.DATETIME <= V_ROW_LS_CCO_SETTINGS.ROP_END_DATE   
                            AND LEG.SERVINGFREQ = LEG.NFREQ
                            AND  (   LEG.RSRP_DIFF = V_ROW_SETTINGS.BAD_POLLUTER_RSRP_DIFF  
                                     AND LEG.POLLUTER_CNT >= V_ROW_SETTINGS.BAD_MIN_NUMBEROFPOLLUTER
                                   -- AND LEG.RSRP_AVG >= V_ROW_SETTINGS.BAD_RSRP_THRESHOLD 
                                   -- AND LEG.RSRQ_AVG <= V_ROW_SETTINGS.BAD_RSRQ_THRESHOLD     
                                 ) 
        ),
      geo_data1_agg1 as 
      (
        SELECT  
              CELLID,
              NCELLID, 
              PLATITUDE,
              PLONGITUDE,
              SUM(POLLUTER_CNT) AS SUM_OF_POLLUTERCNT,
              BAD_QUAL_WORKING_BIN_INDEX
        from geo_data1
         GROUP BY CELLID,PLATITUDE,PLONGITUDE,NCELLID,BAD_QUAL_WORKING_BIN_INDEX
      ),  
      COORDINATE as  
      (         SELECT 
                   ROUND(CALCDISTANCE_JAVA(MAX(B.LATITUDE),MIN(LONGITUDE),MIN(LATITUDE),MIN(LONGITUDE)) /(  v_BinSizeInMeter   /1000),0) VERTICAL_BIN_COUNT,
                   ROUND(CALCDISTANCE_JAVA(MAX(B.LATITUDE),MIN(LONGITUDE),MAX(LATITUDE),MAX(LONGITUDE)) /(  v_BinSizeInMeter   /1000),0) HORIZONTAL_BIN_COUNT,
                   MIN(B.LATITUDE) SELAT,
                   MAX(B.LATITUDE) NWLAT,
                   MAX(B.LONGITUDE) SELON,
                   MIN(B.LONGITUDE) NWLON
                 FROM LS_CCO_MT_BORDER_LIST B
                    WHERE B.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP     AND B.EXECUTIONGUID = V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID 
      ),  
   vResult1 
   as (
     select TRUNC (COORDINATE.VERTICAL_BIN_COUNT *   (PLATITUDE - COORDINATE.SELAT) / (COORDINATE.NWLAT - COORDINATE.SELAT)) X_INDEX,
           TRUNC (COORDINATE.HORIZONTAL_BIN_COUNT * (PLONGITUDE - COORDINATE.NWLON) / (COORDINATE.SELON - COORDINATE.NWLON)) Y_INDEX,
           GEO_DATA1_AGG1.CELLID,
           GEO_DATA1_AGG1.NCELLID,  
           SUM(SUM_OF_POLLUTERCNT) AS SUM_POLLUTERCNT,
            BAD_QUAL_WORKING_BIN_INDEX
       from geo_data1_agg1, COORDINATE 
        WHERE 1=1
        AND PLATITUDE IS NOT NULL
        AND PLATITUDE>COORDINATE.SELAT AND PLATITUDE<COORDINATE.NWLAT
        AND PLONGITUDE>COORDINATE.NWLON AND PLONGITUDE<COORDINATE.SELON
    GROUP BY TRUNC (COORDINATE.VERTICAL_BIN_COUNT * (PLATITUDE - COORDINATE.SELAT) / (COORDINATE.NWLAT - COORDINATE.SELAT)) ,
             TRUNC (COORDINATE.HORIZONTAL_BIN_COUNT * (PLONGITUDE - COORDINATE.NWLON) / (COORDINATE.SELON - COORDINATE.NWLON)),
           GEO_DATA1_AGG1.CELLID, 
           GEO_DATA1_AGG1.NCELLID,
           GEO_DATA1_AGG1.BAD_QUAL_WORKING_BIN_INDEX
        ) ,
       V_result2 as (
        select 
           X_INDEX,
           Y_INDEX,
           CELLID,
           NCELLID,
           SUM_POLLUTERCNT, 
           SUM (SUM_POLLUTERCNT) OVER(PARTITION BY CELLID,  BAD_QUAL_WORKING_BIN_INDEX) AS SERVING_TOTAL_COUNT,
           SUM (SUM_POLLUTERCNT) OVER(PARTITION BY NCELLID, BAD_QUAL_WORKING_BIN_INDEX) AS NCELL_TOTAL_POLLUTER_COUNT,
           BAD_QUAL_WORKING_BIN_INDEX
           FROM vResult1
        ),
       v_result3 as
       (
        select 
         V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,
         V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID,
         X_INDEX,
         Y_INDEX,
         CELLID,
         NCELLID,
         SUM_POLLUTERCNT,
         SERVING_TOTAL_COUNT,
         NCELL_TOTAL_POLLUTER_COUNT,
         BAD_QUAL_WORKING_BIN_INDEX,
          DENSE_RANK() OVER (PARTITION BY BAD_QUAL_WORKING_BIN_INDEX ORDER BY SERVING_TOTAL_COUNT DESC, CELLID asc) SOURCE_RANK,
         DENSE_RANK() OVER (PARTITION BY BAD_QUAL_WORKING_BIN_INDEX ORDER BY NCELL_TOTAL_POLLUTER_COUNT DESC, NCELLID asc) NEI_RANK
         from   V_result2
        )
        select t.* from v_result3 t where NEI_RANK = V_ROW_SETTINGS.BAD_MIN_NUMBEROFPOLLUTER-1;
           
           COMMIT;
                   
        MERGE INTO LS_CCO_MT_CELL  T
        USING       
              (      with v_data as (
                     SELECT /*+  ORDERED USE_HASH(C,BIN) MATERIALIZE */  
                                         C.CELLID,C.NCELLID,  
                                         BIN.BAD_QUAL_WORKING_BIN_INDEX, --,C.BADQUALITY_COST
                                         SUM_OF_POLLUTERCNT, 
                                         SERVING_TOTAL_COUNT, 
                                         NCELL_TOTAL_POLLUTER_COUNT,
                                         NULL AS CANDIDATECELLCOST,
                                         BIN.BAD_QUAL_WORKING_BIN_CENTER_LAT,
                                         BIN.BAD_QUAL_WORKING_BIN_CENTER_LON,
                                         row_number() over(partition by BIN.BAD_QUAL_WORKING_BIN_INDEX order by NCELL_TOTAL_POLLUTER_COUNT desc, SERVING_TOTAL_COUNT desc ) AS mm_row
                                        FROM LS_CCO_MT_BIN_REL_LIST C   
                                        JOIN LS_CCO_MT_BIN_LIST BIN
                                            ON      C.EXECUTIONSTARTTIMESTAMP=BIN.EXECUTIONSTARTTIMESTAMP
                                                AND C.EXECUTIONGUID=BIN.EXECUTIONGUID
                                                AND C.X_INDEX=BIN.X_INDEX
                                                AND C.Y_INDEX=BIN.Y_INDEX 
                                        WHERE 
                                            BIN.BAD_QUAL_WORKING_BIN_INDEX IS NOT NULL
                                        -- AND   ACCUMULATED_BADQUALITY_COST > 0 
                                        AND C.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID  
                                        AND C.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP 
                                    )
                        select   NCELLID   ,
                         LISTAGG('Problem Area - Bin index: '||BAD_QUAL_WORKING_BIN_INDEX ||' Bin Location:'|| ROUND(BAD_QUAL_WORKING_BIN_CENTER_LAT,6) ||' , '||ROUND(BAD_QUAL_WORKING_BIN_CENTER_LON,6)||' Cost:'||CANDIDATECELLCOST,' ; ')
                           WITHIN GROUP (ORDER BY NCELLID)  REASON
                           FROM v_data 
                           WHERE MM_ROW=1 
                           GROUP BY NCELLID   
                 ) src 
                 on (t.cellid = src.ncellid)
                 when matched then update set 
                 t.ISBADQUALITY =1,
                 T.REASON_BADQUALITY  = substr(src.REASON,1,800)
            WHERE   T.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID  AND T.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP ;
    
    COMMIT;
END;

PROCEDURE SET_GLOBAL_PARAMS 
IS
BEGIN 
    SELECT MIN(ROP_STARTTIME), MAX(ROP_ENDTIME) ,max(BAD_QUALITY_SWITCH), max(WEAK_COVERAGE_SWITCH)
            INTO 
            V_ROW_LS_CCO_SETTINGS.ROP_START_DATE,  
            V_ROW_LS_CCO_SETTINGS.ROP_END_DATE , 
             V_ROW_LS_CCO_SETTINGS.BAD_QUALITY_SWITCH,    
             V_ROW_LS_CCO_SETTINGS.WEAK_COVERAGE_SWITCH
     FROM  LS_CCO_MT_GENERAL_SETTINGS T 
    WHERE  T.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP  AND T.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID ;
    
   /***************************************************************************************/ 
   
   SELECT   SCHEDULEID INTO  V_ROW_LS_CCO_SETTINGS.SCHEDULER_PERIOD 
   FROM PISON_EXECUTION T
   WHERE  T.EXECUTIONSTARTTIMESTAMP = V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP  AND T.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID ;

    
END;

PROCEDURE   FILL_OSS_TABLES_V2
IS
v_Action_Count NUMBER :=0;
    v_jobname            CONSTANT VARCHAR2 (100 BYTE) := LITESON_HELPERS.GET_EXECUTIONPLAN_PARAM (V_ROW_LS_CCO_SETTINGS.EXECUTIONPROFILE, 'Name');
    v_jobdesc            CONSTANT VARCHAR2 (100 BYTE) := LITESON_HELPERS.GET_EXECUTIONPLAN_PARAM (V_ROW_LS_CCO_SETTINGS.EXECUTIONPROFILE,  'Description');  

BEGIN

 SELECT COUNT(1) INTO v_Action_Count FROM V_LS_CCO_MT_OSS_SOLUTION T 
                                        WHERE T.EXECUTIONGUID=V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID 
                                         AND T.EXECUTIONSTARTTIMESTAMP=V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP AND ROWNUM <5;

   IF v_Action_Count > 0 THEN
     LITESON_HELPERS.FILL_OSSSRV_JOBS_QUEUE_TABLES (V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID,
                                                  V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP,
                                                  v_jobname,
                                                  v_jobdesc,
                                                  'V_LS_CCO_MT_OSS_SOLUTION',
                                                  V_ROW_LS_CCO_SETTINGS.OPERATION_TYPE,
                                                  V_ROW_LS_CCO_SETTINGS.EXECUTIONPARAMS,
                                                  NULL);
 END IF; 
   COMMIT;
END;

PROCEDURE INIT_GEO_PROCESS
IS 
BEGIN

     IF V_ROW_LS_CCO_SETTINGS.BAD_QUALITY_SWITCH='true' or  V_ROW_LS_CCO_SETTINGS.WEAK_COVERAGE_SWITCH='true' then
     
        FILL_BORDER_LIST;
        FILL_BIN_CELL_LIST;
        
        IF V_ROW_LS_CCO_SETTINGS.WEAK_COVERAGE_SWITCH='true' THEN 
        FILL_WEAKCOV_WORKING_BINS;
        FILL_WEAKCOV_CANDIDATE_LIST;
        END IF;
       
        IF V_ROW_LS_CCO_SETTINGS.BAD_QUALITY_SWITCH='true' THEN 
        FILL_BAD_QUAL_WORKING_BINS;
        FILL_BADQUAL_CANDIDATE_LIST;
        END IF;

     END IF;
END;

  PROCEDURE INIT (EXECUTIONGUID             IN     RAW,
                  EXECUTIONSTARTTIMESTAMP   IN     TIMESTAMP,
                  EXECNO                    IN     NUMBER,
                  FIRSTEXECUTION            IN     NUMBER,
                  LASTEXECUTION             IN     NUMBER,
                  EXECUTIONPROFILE          IN     XMLTYPE,
                  GENERICMETADATA           IN     XMLTYPE,
                  KPICMMAPPINGS             IN     XMLTYPE,
                  MACHINEOS                 IN     VARCHAR2,
                  AUTHINFO                  IN     VARCHAR2,
                  OPERATIONTYPE             IN     NUMBER,
                  EXECUTIONTIME             IN     TIMESTAMP,
                  RESULTCODE                   OUT NUMBER,
                  OBSERVATIONPERIODS        IN     XMLTYPE,
                  EXECUTIONPARAMS           IN     XMLTYPE
                  )
   IS
     
    v_code                        NUMBER;
    v_errm                        VARCHAR2 (4000 BYTE);
    v_jobname            CONSTANT VARCHAR2 (100 BYTE) := LITESON_HELPERS.GET_EXECUTIONPLAN_PARAM (INIT.EXECUTIONPROFILE, 'Name');
    v_jobdesc            CONSTANT VARCHAR2 (100 BYTE) := LITESON_HELPERS.GET_EXECUTIONPLAN_PARAM (INIT.EXECUTIONPROFILE,  'Description');  
    v_EXTRAINFO          VARCHAR2 (500);
    v_Action_Count                NUMBER; 
    V_OPTIMIZER_NAME     CONSTANT PISON_OPTIMIZER.OPTIMIZERNAME%TYPE := 'UCCO_MT';
   BEGIN
        v_EXTRAINFO := 'ModuleName=' ||  V_OPTIMIZER_NAME;
        
        LITESON_HELPERS.UPDATE_STATE_TABLE (INIT.EXECUTIONGUID,  INIT.EXECUTIONSTARTTIMESTAMP, 'Optimizer has been started');
   
        LITESON_HELPERS.FILL_LOG_LITESON_INPUT(
                                              INIT.EXECUTIONGUID,INIT.EXECUTIONSTARTTIMESTAMP,INIT.EXECNO,INIT.FIRSTEXECUTION,INIT.LASTEXECUTION,
                                              INIT.EXECUTIONPROFILE,GENERICMETADATA,MACHINEOS,AUTHINFO,121000,-1,INIT.OPERATIONTYPE,
                                              INIT.EXECUTIONPARAMS,INIT.OBSERVATIONPERIODS
                                              ); 
     
      V_ROW_LS_CCO_SETTINGS.EXECUTIONSTARTTIMESTAMP:=INIT.EXECUTIONSTARTTIMESTAMP;
      V_ROW_LS_CCO_SETTINGS.EXECUTIONGUID     :=INIT.EXECUTIONGUID;
      V_ROW_LS_CCO_SETTINGS.EXECUTIONPLANID   :=ABS(LITESON_HELPERS.GET_EXECUTIONPLAN_PARAM (INIT.EXECUTIONPROFILE, 'Id'));
      V_ROW_LS_CCO_SETTINGS.OPTIMIZER_NAME    :=V_OPTIMIZER_NAME;
      --V_ROW_LS_CCO_SETTINGS.IS_FORCE_ROLLBACK :=CAST(LITESON_HELPERS.GET_EXECUTIONPARAMS(INIT.EXECUTIONPARAMS,'ForceRollback') AS NUMBER) ;
      --V_ROW_LS_CCO_SETTINGS.IS_PERIOD_ROLLBACK:=CAST(LITESON_HELPERS.GET_EXECUTIONPARAMS(INIT.EXECUTIONPARAMS,'CurrentScheduleIndex') AS NUMBER);
       
      V_ROW_LS_CCO_SETTINGS.EXECUTIONPROFILE  :=INIT.EXECUTIONPROFILE;
      V_ROW_LS_CCO_SETTINGS.EXECUTIONPARAMS   :=INIT.EXECUTIONPARAMS;
      V_ROW_LS_CCO_SETTINGS.OBSERVATIONPERIODS:=INIT.OBSERVATIONPERIODS;
      V_ROW_LS_CCO_SETTINGS.OPERATION_TYPE    :=INIT.OPERATIONTYPE;
      V_ROW_LS_CCO_SETTINGS.SESSION_ID        :=SYS_CONTEXT('USERENV', 'SID');
 
    
    TRUNCATE_TEMP_TABLES;
    
    FILL_SETTINGS_TABLE;  
    SET_GLOBAL_PARAMS;
    FILL_TEMP_RELATION_TABLE;
    FILL_CELL_ALL_KPIS; 
    FILL_CELL_ALL_ACTIONS; 
    FILL_RELATIONS;
        
   /* IF (( V_ROW_LS_CCO_SETTINGS.IS_FORCE_ROLLBACK = 1 OR V_ROW_LS_CCO_SETTINGS.IS_PERIOD_ROLLBACK = 1 ))  */
   IF (V_ROW_LS_CCO_SETTINGS.SCHEDULER_PERIOD  IN ('Rollback','ForceRollback') ) 
    THEN   
        FILL_ROLLBACK_ACTION_REPORT;
        FILL_ROLLBACK_SOLUTIONS;
        DELETE_HIST_AFTER_ROLLBACK;
   ELSE
        CALCULATE_TA_VALUES; 
        UPDATE_RELATION_BASED_KPIS;     
        FILL_GAP_OVERSHOOT_REPORT;
        INIT_GEO_PROCESS;
        UNDERSHOOT_CELL_PRE_ACTION;
        OVERSHOOT_CELL_PRE_ACTION;             
        FIND_CELL_POWER_TILT_REPORT;  
        FILL_RET_ACTION_REPORT;
        --FILL_POWER_ACTION_REPORT;
        FILL_TILT_ACTION_REPORT;
        FILL_ACTION_SOLUTIONS;     
        FILL_ORIGINAL_VALUES;                         
    END IF;
    
    FILL_OSS_TABLES_V2;
    TRUNCATE_TEMP_TABLES;
    LITESON_HELPERS.UPDATE_STATE_TABLE (INIT.EXECUTIONGUID, INIT.EXECUTIONSTARTTIMESTAMP, 'Optimizer has been finished'); 
   END;

END LS_CCO_MT;
