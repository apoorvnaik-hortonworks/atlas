package org.apache.atlas.model.generator;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasJson;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class Driver {

    private static final String MODEL_NAME = "model";
    private static final String BASE_MODEL = "baseModel";

    private static CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();
        Option  opt;

        opt = new Option(MODEL_NAME, true, "Model JSON file");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option(BASE_MODEL, true, "Base Model JSON file");
        opt.setRequired(true);
        options.addOption(opt);

        return new GnuParser().parse(options, args);
    }

    public static void main(String[] args) throws ParseException, IOException, AtlasBaseException {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.DEBUG);

        CommandLine   commandLine     = parseArgs(args);
        String        baseModelFile   = commandLine.getOptionValue(BASE_MODEL);
        String        modelFileName   = commandLine.getOptionValue(MODEL_NAME);
        String        baseModelJson   = FileUtils.readFileToString(new File(baseModelFile));
        String        modelJson       = FileUtils.readFileToString(new File(modelFileName));
        AtlasTypesDef baseTypeDefs    = AtlasJson.fromJson(baseModelJson, AtlasTypesDef.class);
        AtlasTypesDef modelTypesDef   = AtlasJson.fromJson(modelJson, AtlasTypesDef.class);

        // Register all type definitions
        AtlasTypeRegistry typeRegistry = new AtlasTypeRegistry();
        AtlasTypeRegistry.AtlasTransientTypeRegistry transientTypeRegistry = typeRegistry.lockTypeRegistryForUpdate();
        transientTypeRegistry.addTypes(baseTypeDefs);
        transientTypeRegistry.addTypes(modelTypesDef);
        typeRegistry.releaseTypeRegistryForUpdate(transientTypeRegistry, true);

        ModelClassGenerator modelClassGenerator = new ModelClassGenerator(typeRegistry);
        modelClassGenerator.process();
    }
}
