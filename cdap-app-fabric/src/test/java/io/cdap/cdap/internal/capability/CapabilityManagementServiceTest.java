/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.internal.capability;

import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.CapabilityAppWithWorkflow;
import io.cdap.cdap.CapabilitySleepingWorkflowApp;
import io.cdap.cdap.WorkflowAppWithFork;
import io.cdap.cdap.api.annotation.Requirements;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Test for CapabilityManagementService
 */
public class CapabilityManagementServiceTest extends AppFabricTestBase {

  private static ArtifactRepository artifactRepository;
  private static LocationFactory locationFactory;
  private static CConfiguration cConfiguration;
  private static CapabilityManagementService capabilityManagementService;
  private static ApplicationLifecycleService applicationLifecycleService;
  private static ProgramLifecycleService programLifecycleService;
  private static CapabilityStatusStore capabilityStatusStore;
  private static final Gson GSON = new Gson();

  @BeforeClass
  public static void setup() {
    locationFactory = getInjector().getInstance(LocationFactory.class);
    artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    cConfiguration = getInjector().getInstance(CConfiguration.class);
    capabilityManagementService = getInjector().getInstance(CapabilityManagementService.class);
    capabilityStatusStore = new CapabilityStatusStore(getInjector().getInstance(TransactionRunner.class));
    applicationLifecycleService = getInjector().getInstance(ApplicationLifecycleService.class);
    programLifecycleService = getInjector().getInstance(ProgramLifecycleService.class);
  }

  @AfterClass
  public static void stop() {
    AppFabricTestHelper.shutdown();
  }

  @Test
  public void testCapabilityManagement() throws Exception {
    String appName = AllProgramsApp.NAME;
    String programName = AllProgramsApp.NoOpService.NAME;
    Class<AllProgramsApp> appClass = AllProgramsApp.class;
    String version = "1.0.0";
    String namespace = NamespaceId.SYSTEM.getNamespace();
    //deploy the artifact
    deployTestArtifact(namespace, appName, version, appClass);

    ApplicationId applicationId = new ApplicationId(namespace, appName, version);
    ProgramId programId = new ProgramId(applicationId, ProgramType.SERVICE, programName);
    String externalConfigPath = tmpFolder.newFolder("capability-config-test").getAbsolutePath();
    cConfiguration.set(Constants.Capability.CONFIG_DIR, externalConfigPath);
    String fileName = "cap1.json";
    File testJson = new File(
      CapabilityManagementServiceTest.class.getResource(String.format("/%s", fileName)).getPath());
    Files.copy(testJson, new File(externalConfigPath, fileName));
    capabilityManagementService.runTask();
    List<JsonObject> appList = getAppList(namespace);
    Assert.assertFalse(appList.isEmpty());
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 1);

    //remove the file and make sure it gets removed
    new File(externalConfigPath, fileName).delete();
    capabilityManagementService.runTask();
    Assert.assertTrue(getAppList(namespace).isEmpty());

    //insert a pending entry, this should get re-applied and enable the capability
    FileReader fileReader = new FileReader(testJson);
    CapabilityConfig capabilityConfig = GSON.fromJson(fileReader, CapabilityConfig.class);
    capabilityStatusStore.addOrUpdateCapabilityOperation("cap1", CapabilityAction.ENABLE, capabilityConfig);
    capabilityManagementService.runTask();
    Assert.assertTrue(capabilityStatusStore.isEnabled("cap1"));
    Assert.assertTrue(capabilityStatusStore.getCapabilityOperations().isEmpty());

    //pending task out of the way , on next run should be deleted
    capabilityManagementService.runTask();
    Assert.assertFalse(capabilityStatusStore.isEnabled("cap1"));

    //disable from delete and see if it is applied correctly
    CapabilityConfig disableConfig = new CapabilityConfig(capabilityConfig.getLabel(), CapabilityStatus.DISABLED,
                                                          capabilityConfig.getCapability(),
                                                          capabilityConfig.getApplications(),
                                                          capabilityConfig.getPrograms());
    writeConfigAsFile(externalConfigPath, fileName, disableConfig);
    capabilityManagementService.runTask();
    Assert.assertFalse(capabilityStatusStore.isEnabled("cap1"));
    Assert.assertEquals(disableConfig, capabilityStatusStore.getConfig("cap1"));

    //enable again
    writeConfigAsFile(externalConfigPath, fileName, capabilityConfig);
    capabilityManagementService.runTask();
    Assert.assertTrue(capabilityStatusStore.isEnabled("cap1"));
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 1);

    //cleanup
    new File(externalConfigPath, fileName).delete();
    capabilityManagementService.runTask();
    artifactRepository.deleteArtifact(Id.Artifact.from(new Id.Namespace(namespace), appName, version));
  }

  private void writeConfigAsFile(String externalConfigPath, String fileName,
                                 CapabilityConfig disableConfig) throws IOException {
    try (FileWriter writer = new FileWriter(new File(externalConfigPath, fileName))) {
      GSON.toJson(disableConfig, writer);
    }
  }

  @Test
  public void testCapabilityRefresh() throws Exception {
    String externalConfigPath = tmpFolder.newFolder("capability-config-refresh").getAbsolutePath();
    cConfiguration.set(Constants.Capability.CONFIG_DIR, externalConfigPath);

    String appName = AllProgramsApp.NAME;
    String programName = AllProgramsApp.NoOpService.NAME;
    Class<AllProgramsApp> appClass = AllProgramsApp.class;
    String version = "1.0.0";
    String namespace = NamespaceId.SYSTEM.getNamespace();
    //deploy the artifact
    deployTestArtifact(namespace, appName, version, appClass);

    ApplicationId applicationId = new ApplicationId(namespace, appName, version);
    ProgramId programId = new ProgramId(applicationId, ProgramType.SERVICE, programName);
    //check that app is not available
    List<JsonObject> appList = getAppList(namespace);
    Assert.assertTrue(appList.isEmpty());

    //enable the capability
    CapabilityConfig config = getTestConfig();
    writeConfigAsFile(externalConfigPath, config.getCapability(), config);
    capabilityManagementService.runTask();
    //app should show up and program should have run
    appList = getAppList(namespace);
    Assert.assertFalse(appList.isEmpty());
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 1);
    String capability = config.getCapability();
    Assert.assertTrue(capabilityStatusStore.isEnabled(capability));

    //disable capability. Program should stop, status should be disabled and app should still be present.
    CapabilityConfig disabledConfig = changeConfigStatus(config, CapabilityStatus.DISABLED);
    writeConfigAsFile(externalConfigPath, disabledConfig.getCapability(), disabledConfig);
    capabilityManagementService.runTask();
    assertProgramRuns(programId, ProgramRunStatus.KILLED, 1);
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 0);
    Assert.assertFalse(capabilityStatusStore.isEnabled(capability));
    appList = getAppList(namespace);
    Assert.assertFalse(appList.isEmpty());

    //delete capability. Program should stop, status should be disabled and app should still be present.
    new File(externalConfigPath, disabledConfig.getCapability()).delete();
    capabilityManagementService.runTask();
    Assert.assertNull(capabilityStatusStore.getStatus(capability));
    appList = getAppList(namespace);
    Assert.assertTrue(appList.isEmpty());
    artifactRepository.deleteArtifact(Id.Artifact
                                        .from(new Id.Namespace("system"), appName, version));
  }

  @Test
  public void testApplicationDeployment() throws Exception {
    String externalConfigPath = tmpFolder.newFolder("capability-config-app").getAbsolutePath();
    cConfiguration.set(Constants.Capability.CONFIG_DIR, externalConfigPath);
    capabilityManagementService.runTask();
    String testVersion = "1.0.0";
    //Deploy application with capability
    Class<CapabilityAppWithWorkflow> appWithWorkflowClass = CapabilityAppWithWorkflow.class;
    Requirements declaredAnnotation = appWithWorkflowClass.getDeclaredAnnotation(Requirements.class);
    //verify this app has capabilities
    Assert.assertTrue(declaredAnnotation.capabilities().length > 0);
    Assert.assertFalse(capabilityStatusStore.isEnabled(declaredAnnotation.capabilities()[0]));
    String appNameWithCapability = appWithWorkflowClass.getSimpleName() + UUID.randomUUID();
    deployTestArtifact(Id.Namespace.DEFAULT.getId(), appNameWithCapability, testVersion, appWithWorkflowClass);
    try {
      //deploy app
      Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, appNameWithCapability, testVersion);
      applicationLifecycleService
        .deployApp(NamespaceId.DEFAULT, appNameWithCapability, testVersion, artifactId,
                   null, programId -> {
          });
      Assert.fail("Expecting exception");
    } catch (CapabilityNotAvailableException ex) {
      //expected
    }

    //Deploy application without capability
    Class<WorkflowAppWithFork> appNoCapabilityClass = WorkflowAppWithFork.class;
    Requirements declaredAnnotation1 = appNoCapabilityClass.getDeclaredAnnotation(Requirements.class);
    //verify this app has no capabilities
    Assert.assertNull(declaredAnnotation1);
    String appNameWithOutCapability = appWithWorkflowClass.getSimpleName() + UUID.randomUUID();
    deployArtifactAndApp(appNoCapabilityClass, appNameWithOutCapability, testVersion);

    //enable the capabilities
    List<CapabilityConfig> capabilityConfigs = Arrays.stream(declaredAnnotation.capabilities())
      .map(capability -> new CapabilityConfig("Test capability", CapabilityStatus.ENABLED, capability,
                                              Collections.emptyList(), Collections.emptyList()))
      .collect(Collectors.toList());
    for (CapabilityConfig capabilityConfig : capabilityConfigs) {
      writeConfigAsFile(externalConfigPath, capabilityConfig.getCapability(), capabilityConfig);
    }
    capabilityManagementService.runTask();

    //deployment should go through now
    Id.Artifact artifactId = Id.Artifact
      .from(Id.Namespace.DEFAULT, appNameWithCapability, testVersion);
    applicationLifecycleService
      .deployApp(NamespaceId.DEFAULT, appNameWithCapability, testVersion, artifactId,
                 null, programId -> {
        });

    applicationLifecycleService.removeApplication(NamespaceId.DEFAULT.app(appNameWithCapability, testVersion));
    applicationLifecycleService.removeApplication(NamespaceId.DEFAULT.app(appNameWithOutCapability, testVersion));
    artifactRepository.deleteArtifact(Id.Artifact
                                        .from(new Id.Namespace(NamespaceId.DEFAULT.getNamespace()),
                                              appNameWithCapability, testVersion));
    artifactRepository.deleteArtifact(Id.Artifact
                                        .from(new Id.Namespace(NamespaceId.DEFAULT.getNamespace()),
                                              appNameWithOutCapability, testVersion));

    for (CapabilityConfig capabilityConfig : capabilityConfigs) {
      new File(externalConfigPath, capabilityConfig.getCapability()).delete();
    }
    capabilityManagementService.runTask();
  }

  @Test
  public void testProgramStart() throws Exception {
    String externalConfigPath = tmpFolder.newFolder("capability-config-program").getAbsolutePath();
    cConfiguration.set(Constants.Capability.CONFIG_DIR, externalConfigPath);
    String appName = CapabilitySleepingWorkflowApp.NAME;
    Class<CapabilitySleepingWorkflowApp> appClass = CapabilitySleepingWorkflowApp.class;
    String version = "1.0.0";
    String namespace = "default";
    //deploy the artifact
    deployTestArtifact(namespace, appName, version, appClass);

    //enable a capability with no system apps and programs
    CapabilityConfig enabledConfig = new CapabilityConfig("Enable healthcare", CapabilityStatus.ENABLED,
                                                          "healthcare", Collections.emptyList(),
                                                          Collections.emptyList());
    writeConfigAsFile(externalConfigPath, enabledConfig.getCapability(), enabledConfig);
    capabilityManagementService.runTask();
    String capability = enabledConfig.getCapability();
    Assert.assertTrue(capabilityStatusStore.isEnabled(capability));

    //deploy an app with this capability and start a workflow
    ApplicationId applicationId = new ApplicationId(namespace, appName);
    Id.Artifact artifactId = Id.Artifact
      .from(new Id.Namespace(namespace), appName, version);
    ApplicationWithPrograms applicationWithPrograms = applicationLifecycleService
      .deployApp(new NamespaceId(namespace), appName, null, artifactId, null, op -> {
      });
    Iterable<ProgramDescriptor> programs = applicationWithPrograms.getPrograms();
    for (ProgramDescriptor program : programs) {
      programLifecycleService.start(program.getProgramId(), new HashMap<>(), false);
    }
    ProgramId programId = new ProgramId(applicationId, ProgramType.WORKFLOW,
                                        CapabilitySleepingWorkflowApp.SleepWorkflow.class.getSimpleName());
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 1);

    //disable the capability -  the program that was started should stop
    CapabilityConfig disabledConfig = new CapabilityConfig("Disable healthcare", CapabilityStatus.DISABLED,
                                                           "healthcare", Collections.emptyList(),
                                                           Collections.emptyList());
    writeConfigAsFile(externalConfigPath, capability, disabledConfig);
    capabilityManagementService.runTask();
    assertProgramRuns(programId, ProgramRunStatus.KILLED, 1);
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 0);
    Assert.assertFalse(capabilityStatusStore.isEnabled(capability));
    //try starting programs
    for (ProgramDescriptor program : programs) {
      try {
        programLifecycleService.start(program.getProgramId(), new HashMap<>(), false);
      } catch (CapabilityNotAvailableException ex) {
        //expecting exception
      }
    }
    new File(externalConfigPath, capability).delete();
    capabilityManagementService.runTask();
    Assert.assertNull(capabilityStatusStore.getStatus(capability));
    artifactRepository.deleteArtifact(Id.Artifact
                                        .from(new Id.Namespace(namespace), appName, version));
  }

  private CapabilityConfig changeConfigStatus(CapabilityConfig original, CapabilityStatus status) {
    return new CapabilityConfig(original.getLabel(), status, original.getCapability(), original.getApplications(),
                                original.getPrograms());
  }

  private CapabilityConfig getTestConfig() {
    String appName = AllProgramsApp.NAME;
    String programName = AllProgramsApp.NoOpService.NAME;
    String version = "1.0.0";
    String namespace = NamespaceId.SYSTEM.getNamespace();
    String label = "Enable capability";
    String capability = "test";
    ArtifactSummary artifactSummary = new ArtifactSummary(appName, version, ArtifactScope.SYSTEM);
    SystemApplication application = new SystemApplication(namespace, appName, version, artifactSummary, null);
    SystemProgram program = new SystemProgram(namespace, appName, ProgramType.SERVICE.name(),
                                              programName, version, null);
    return new CapabilityConfig(label, CapabilityStatus.ENABLED, capability,
                                Collections.singletonList(application), Collections.singletonList(program));
  }

  void deployTestArtifact(String namespace, String appName, String version, Class<?> appClass) throws Exception {
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.from(namespace), appName, version);
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, appClass);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Files.copy(Locations.newInputSupplier(appJar), appJarFile);
    appJar.delete();
    artifactRepository.addArtifact(artifactId, appJarFile);
  }

  private void deployArtifactAndApp(Class<?> applicationClass, String appName, String testVersion) throws Exception {
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, appName, testVersion);
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, applicationClass);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Files.copy(Locations.newInputSupplier(appJar), appJarFile);
    appJar.delete();
    artifactRepository.addArtifact(artifactId, appJarFile);
    //deploy app
    applicationLifecycleService
      .deployApp(NamespaceId.DEFAULT, appName, testVersion, artifactId,
                 null, programId -> {
        });
  }
}
