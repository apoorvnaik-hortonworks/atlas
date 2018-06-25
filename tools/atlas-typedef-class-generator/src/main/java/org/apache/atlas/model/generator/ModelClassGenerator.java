package org.apache.atlas.model.generator;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.type.*;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.lang.model.element.Modifier;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

public class ModelClassGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(ModelClassGenerator.class);

    private static final String PACKAGE_NAME = "org.apache.atlas.model.generated";

    private Map<String, TypeSpec> processedTypes  = new HashMap<>();
    private Map<String, String>   typeToClassName = new HashMap<>();

    private AtlasTypeRegistry atlasTypeRegistry;

    public ModelClassGenerator(AtlasTypeRegistry typeRegistry) {

        this.atlasTypeRegistry = typeRegistry;

        Collection<AtlasEnumDef>           allEnums           = typeRegistry.getAllEnumDefs();
        Collection<AtlasStructDef>         allStructs         = typeRegistry.getAllStructDefs();
        Collection<AtlasClassificationDef> allClassifications = typeRegistry.getAllClassificationDefs();
        Collection<AtlasEntityDef>         allEntities        = typeRegistry.getAllEntityDefs();

        LOG.info("{} enums", allEnums.size());
        LOG.info("{} structs", allStructs.size());
        LOG.info("{} classifications", allClassifications.size());
        LOG.info("{} entities", allEntities.size());

    }

    public void process() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> ModelClassGenerator.process()");
        }

        processEnumTypes();
        processStructTypes();
        processClassificationDefs();
        processEntityTypes();

        // TODO: Maybe add handling for relationships as well

        for (TypeSpec value: processedTypes.values()) {
            JavaFile javaFile = JavaFile.builder("org.apache.atlas.generated", value).build();
            javaFile.writeTo(System.out);
            System.out.println();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== ModelClassGenerator.process()");
        }
    }

    private void processEntityTypes() {
        for (AtlasEntityType entityType: atlasTypeRegistry.getAllEntityTypes()) {
            if (processedTypes.containsKey(entityType.getTypeName())) {
                continue;
            }

            processEntityType(entityType);
        }
    }

    private void processEntityType(final AtlasEntityType entityType) {
        String typeName = entityType.getTypeName();
        LOG.info("Modeling entity class for {}", typeName);

        String className = convertToClassName(typeName);
        typeToClassName.put(typeName, className);

        TypeSpec.Builder entityBuilder = TypeSpec.classBuilder(className).addModifiers(Modifier.PUBLIC);
        Set<String>      superTypes    = entityType.getSuperTypes();

        // generate statements for basic attributes
        if (CollectionUtils.isEmpty(superTypes)) {
            processBaseAttributes(entityBuilder);
        }

        // generate attribute statements for all supertypes
        processSupertypes(entityBuilder, superTypes, true);

        // generate statements for classification attributes
        processDeclaredAttributes(entityType, entityBuilder);

        processedTypes.put(typeName, entityBuilder.build());
    }

    private void processClassificationDefs() {
        for (AtlasClassificationType classificationType: atlasTypeRegistry.getAllClassificationTypes()) {
            if (processedTypes.containsKey(classificationType.getTypeName())) {
                continue;
            }

            processClassificationType(classificationType);
        }
    }

    private void processClassificationType(final AtlasClassificationType classificationType) {
        String typeName = classificationType.getTypeName();

        LOG.info("Modeling classification class for {}", typeName);

        String className = convertToClassName(typeName);
        typeToClassName.put(typeName, className);

        TypeSpec.Builder classificationBuilder = TypeSpec.classBuilder(className).addModifiers(Modifier.PUBLIC);
        Set<String>      superTypes            = classificationType.getSuperTypes();

        // generate statements for basic attributes
        if (CollectionUtils.isEmpty(superTypes)) {
            processBaseAttributes(classificationBuilder);
        }

        // generate attribute statements for all supertypes
        processSupertypes(classificationBuilder, superTypes, false);

        // generate statements for classification attributes
        processDeclaredAttributes(classificationType, classificationBuilder);

        processedTypes.put(typeName, classificationBuilder.build());
    }

    private void processSupertypes(final TypeSpec.Builder builder, final Set<String> superTypes, final boolean isEntityType) {
        Set<FieldSpec>  inheritedFieldSpecs  = new HashSet<>();
        Set<MethodSpec> inheritedMethodSpecs = new HashSet<>();

        if (CollectionUtils.isNotEmpty(superTypes)) {
            for (String superType: superTypes) {
                if (!processedTypes.containsKey(superType)) {
                    if (isEntityType) {
                        processEntityType(atlasTypeRegistry.getEntityTypeByName(superType));
                    } else {
                        processClassificationType(atlasTypeRegistry.getClassificationTypeByName(superType));
                    }
                }

                // Merge attributes of superType
                TypeSpec superTypeSpec = processedTypes.get(superType);
                inheritedFieldSpecs.addAll(superTypeSpec.fieldSpecs);
                inheritedMethodSpecs.addAll(superTypeSpec.methodSpecs);
            }

            inheritedFieldSpecs.forEach(builder::addField);
            inheritedMethodSpecs.forEach(builder::addMethod);
        }
    }

    private String convertToClassName(final String typeName) {
        char[] className = new char[typeName.length()];
        if (!Character.isUpperCase(typeName.charAt(0))) {
            className[0] = Character.toUpperCase(typeName.charAt(0));
        } else {
            className[0] = typeName.charAt(0);
        }
        boolean makeUpperCase = false;
        int     classNameIdx  = 1;
        for (int idx = 1; idx < className.length; idx++) {
            if (isLetterOrDigit(typeName.charAt(idx))) {
                char c = typeName.charAt(idx);
                if (makeUpperCase) {
                    className[classNameIdx] = Character.toUpperCase(c);
                    makeUpperCase = false;
                } else {
                    className[classNameIdx] = c;
                }
                classNameIdx++;
            } else {
                makeUpperCase = true;
            }
        }

        String ret = new String(className).trim();
        LOG.info("TypeName = {}, ClassName = {}", typeName, ret);
        return ret;
    }

    private boolean isLetterOrDigit(char c) {
        return (c >= 'a' && c <= 'z') ||
                       (c >= 'A' && c <= 'Z') ||
                       (c >= '0' && c <= '9');
    }

    private TypeName getAttributeType(final AtlasType attributeType) {
        TypeName attrType;

        String attrTypeName = attributeType.getTypeName();

        if (attributeType instanceof AtlasBuiltInTypes.AtlasBooleanType) {
            attrType = TypeName.BOOLEAN;

        } else if (attributeType instanceof AtlasBuiltInTypes.AtlasByteType) {
            attrType = TypeName.BYTE;

        } else if (attributeType instanceof AtlasBuiltInTypes.AtlasShortType) {
            attrType = TypeName.SHORT;

        } else if (attributeType instanceof AtlasBuiltInTypes.AtlasIntType) {
            attrType = TypeName.INT;

        } else if (attributeType instanceof AtlasBuiltInTypes.AtlasLongType) {
            attrType = TypeName.LONG;

        } else if (attributeType instanceof AtlasBuiltInTypes.AtlasDoubleType) {
            attrType = TypeName.DOUBLE;

        } else if (attributeType instanceof AtlasBuiltInTypes.AtlasBigIntegerType) {
            attrType = ClassName.get(BigInteger.class);

        } else if (attributeType instanceof AtlasBuiltInTypes.AtlasBigDecimalType) {
            attrType = ClassName.get(BigDecimal.class);

        } else if (attributeType instanceof AtlasBuiltInTypes.AtlasDateType) {
            attrType = ClassName.get(Date.class);

        } else if (attributeType instanceof AtlasBuiltInTypes.AtlasStringType) {
            attrType = ClassName.get(String.class);

        } else if (attributeType instanceof AtlasArrayType) {
            // If it's an array of entity then process if not done already
            AtlasType elementType = ((AtlasArrayType) attributeType).getElementType();

            if (elementType instanceof AtlasEntityType && !processedTypes.containsKey(elementType.getTypeName())) {
                processEntityType((AtlasEntityType) elementType);
            }

            attrType = ParameterizedTypeName.get(ClassName.get(List.class), getAttributeType(elementType));

        } else if (attributeType instanceof AtlasMapType) {
            AtlasType keyType   = ((AtlasMapType) attributeType).getKeyType();
            AtlasType valueType = ((AtlasMapType) attributeType).getValueType();
            attrType = ParameterizedTypeName.get(ClassName.get(Map.class), getAttributeType(keyType), getAttributeType(valueType));

        } else {
            // Only remaining clause is EntityType / ObjectIdType
            String typeName;
            if (attributeType instanceof AtlasBuiltInTypes.AtlasObjectIdType) {
                typeName = ((AtlasBuiltInTypes.AtlasObjectIdType) attributeType).getObjectType();
            } else {
                typeName = attributeType.getTypeName();
            }

            if (!processedTypes.containsKey(typeName)) {
                processEntityType(atlasTypeRegistry.getEntityTypeByName(typeName));
            }
            attrType = ClassName.get(PACKAGE_NAME, typeToClassName.get(typeName));
        }

        return attrType;
    }

    private void processBaseAttributes(final TypeSpec.Builder builder) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> ModelClassGenerator.processBaseAttributes()");
        }

        // Add all basic attributes
        addFieldWithSetterGetter(builder, "guid", ClassName.get(String.class));
        addFieldWithSetterGetter(builder, "createdBy", ClassName.get(String.class));
        addFieldWithSetterGetter(builder, "updatedBy", ClassName.get(String.class));
        addFieldWithSetterGetter(builder, "createTime", ClassName.get(Date.class));
        addFieldWithSetterGetter(builder, "updateTime", ClassName.get(Date.class));
        addFieldWithSetterGetter(builder, "version", TypeName.LONG);
        addFieldWithSetterGetter(builder, "description", ClassName.get(String.class));
        addFieldWithSetterGetter(builder, "typeVersion", ClassName.get(String.class));

        // Add extra options
        ParameterizedTypeName options = ParameterizedTypeName.get(Map.class, String.class, String.class);
        MethodSpec setter = MethodSpec.methodBuilder("setOptions")
                                      .addModifiers(Modifier.PUBLIC)
                                      .addParameter(options, "options")
                                      .addStatement("this.options = options")
                                      .build();
        MethodSpec getter = MethodSpec.methodBuilder("getOptions")
                                      .addModifiers(Modifier.PUBLIC)
                                      .returns(options)
                                      .addStatement("return options")
                                      .build();
        builder.addField(options, "options", Modifier.PRIVATE);
        builder.addMethod(setter);
        builder.addMethod(getter);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== ModelClassGenerator.processBaseAttributes()");
        }
    }

    private void addFieldWithSetterGetter(final TypeSpec.Builder builder, final String fieldName, final TypeName returnType) {
        String actualFieldName = fieldName;
        if (KeyWordUtils.isKeyword(fieldName)) {
            actualFieldName = fieldName + "$$";
        }

        builder.addField(returnType, actualFieldName, Modifier.PRIVATE);

        String firstLetterToUpper = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        String setterName         = "set" + firstLetterToUpper;
        String getterName         = "get" + firstLetterToUpper;

        MethodSpec setter = MethodSpec.methodBuilder(setterName)
                                      .addModifiers(Modifier.PUBLIC)
                                      .addParameter(returnType, actualFieldName)
                                      .addStatement("this." + actualFieldName + " = " + actualFieldName)
                                      .build();
        MethodSpec getter = MethodSpec.methodBuilder(getterName)
                                      .addModifiers(Modifier.PUBLIC)
                                      .returns(returnType)
                                      .addStatement("return " + actualFieldName)
                                      .build();
        builder.addMethod(getter);
        builder.addMethod(setter);
    }

    private void processStructTypes() {
        for (AtlasStructType structType: atlasTypeRegistry.getAllStructTypes()) {
            if (processedTypes.containsKey(structType.getTypeName())) {
                continue;
            }

            processStructType(structType);
        }
    }

    private void processStructType(AtlasStructType structType) {
        String typeName = structType.getTypeName();

        LOG.info("Modeling Struct class for {}", typeName);

        String className = convertToClassName(typeName);
        typeToClassName.put(typeName, className);

        TypeSpec.Builder structBuilder = TypeSpec.classBuilder(className);

        // generate statements for basic attributes
        processBaseAttributes(structBuilder);

        // generate statements for classification attributes
        processDeclaredAttributes(structType, structBuilder);

        processedTypes.put(typeName, structBuilder.build());
    }

    private void processDeclaredAttributes(final AtlasStructType structType, final TypeSpec.Builder builder) {
        for (AtlasStructType.AtlasAttribute attribute: structType.getAllAttributes().values()) {
            String                                       attributeName = attribute.getName();
            AtlasStructDef.AtlasAttributeDef.Cardinality cardinality   = attribute.getAttributeDef().getCardinality();
            TypeName                                     attributeType = getAttributeType(attribute.getAttributeType());
            ParameterizedTypeName                        parameterizedTypeName;

            switch (cardinality) {
                case SINGLE:
                    addFieldWithSetterGetter(builder, attributeName, attributeType);
                    break;
                case LIST:
                    if (attributeType instanceof ParameterizedTypeName) {
                        parameterizedTypeName = (ParameterizedTypeName) attributeType;
                    } else {
                        parameterizedTypeName = ParameterizedTypeName.get(ClassName.get(List.class), attributeType);
                    }
                    addFieldWithSetterGetter(builder, attributeName, parameterizedTypeName);
                    break;
                case SET:
                    if (attributeType instanceof ParameterizedTypeName) {
                        parameterizedTypeName = (ParameterizedTypeName) attributeType;
                    } else {
                        parameterizedTypeName = ParameterizedTypeName.get(ClassName.get(Set.class), attributeType);
                    }
                    addFieldWithSetterGetter(builder, attributeName, parameterizedTypeName);
                    break;
            }
        }
    }

    private void processEnumTypes() {
        for (AtlasEnumType enumType: atlasTypeRegistry.getAllEnumTypes()) {
            String                                 typeName     = enumType.getTypeName();
            String                                 className    = convertToClassName(typeName);
            TypeSpec.Builder                       enumSpec     = TypeSpec.enumBuilder(className);
            List<AtlasEnumDef.AtlasEnumElementDef> enumElements = enumType.getEnumDef().getElementDefs();
            enumElements.sort(Comparator.comparing(AtlasEnumDef.AtlasEnumElementDef::getOrdinal));
            for (AtlasEnumDef.AtlasEnumElementDef enumElement: enumElements) {
                enumSpec.addEnumConstant(enumElement.getValue());
            }
            processedTypes.put(className, enumSpec.build());
        }
    }

    static class KeyWordUtils {
        private static Set<String> keywords = new HashSet<>();

        static {

            keywords.add("abstract");
            keywords.add("assert");
            keywords.add("boolean");
            keywords.add("break");
            keywords.add("byte");
            keywords.add("case");
            keywords.add("catch");
            keywords.add("char");
            keywords.add("class");
            keywords.add("const");
            keywords.add("continue");
            keywords.add("default");
            keywords.add("do");
            keywords.add("double");
            keywords.add("else");
            keywords.add("extends");
            keywords.add("false");
            keywords.add("final");
            keywords.add("finally");
            keywords.add("float");
            keywords.add("for");
            keywords.add("goto");
            keywords.add("if");
            keywords.add("implements");
            keywords.add("import");
            keywords.add("instanceof");
            keywords.add("int");
            keywords.add("interface");
            keywords.add("long");
            keywords.add("native");
            keywords.add("new");
            keywords.add("null");
            keywords.add("package");
            keywords.add("private");
            keywords.add("protected");
            keywords.add("public");
            keywords.add("return");
            keywords.add("short");
            keywords.add("static");
            keywords.add("strictfp");
            keywords.add("super");
            keywords.add("switch");
            keywords.add("synchronized");
            keywords.add("this");
            keywords.add("throw");
            keywords.add("throws");
            keywords.add("transient");
            keywords.add("true");
            keywords.add("try");
            keywords.add("void");
            keywords.add("volatile");
            keywords.add("while");
        }

        static boolean isKeyword(String s) {
            return s != null && s.length() > 0 && keywords.contains(s);
        }
    }
}
