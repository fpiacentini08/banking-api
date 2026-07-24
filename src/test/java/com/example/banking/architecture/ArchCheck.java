package com.example.banking.architecture;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.classfile.ClassFile;
import java.lang.classfile.ClassModel;
import java.lang.classfile.constantpool.ClassEntry;
import java.lang.classfile.constantpool.PoolEntry;
import java.lang.constant.ClassDesc;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * DIY architecture checker on the JDK ClassFile API (java.lang.classfile, standard since
 * Java 24). Scans compiled classes and extracts every referenced class name from the constant
 * pool (which covers supertypes, thrown/caught types, instantiations, and method-body
 * references) plus the class's own field/method descriptors (which cover types used only in
 * signatures). Rules are package allow-lists over those references.
 */
final class ArchCheck {

    record Violation(String className, String forbiddenReference) {}

    private final Map<String, Set<String>> referencesByClass;

    private ArchCheck(Map<String, Set<String>> referencesByClass) {
        this.referencesByClass = referencesByClass;
    }

    static ArchCheck scan(Path classesDir) {
        Map<String, Set<String>> references = new TreeMap<>();
        try (Stream<Path> files = Files.walk(classesDir)) {
            files.filter(path -> path.toString().endsWith(".class"))
                    .forEach(path -> {
                        ClassModel model = parse(path);
                        references.put(model.thisClass().asInternalName().replace('/', '.'),
                                referencesOf(model));
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new ArchCheck(references);
    }

    /** Classes under packagePrefix may reference only classes under the allowedPrefixes. */
    List<Violation> violations(String packagePrefix, List<String> allowedPrefixes) {
        List<Violation> violations = new ArrayList<>();
        referencesByClass.forEach((className, references) -> {
            if (!className.startsWith(packagePrefix)) {
                return;
            }
            for (String reference : references) {
                if (allowedPrefixes.stream().noneMatch(reference::startsWith)) {
                    violations.add(new Violation(className, reference));
                }
            }
        });
        return violations;
    }

    /** True when any class under fromPackagePrefix references a class under toPackagePrefix. */
    boolean referenceExists(String fromPackagePrefix, String toPackagePrefix) {
        return referencesByClass.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(fromPackagePrefix))
                .flatMap(entry -> entry.getValue().stream())
                .anyMatch(reference -> reference.startsWith(toPackagePrefix));
    }

    private static ClassModel parse(Path path) {
        try {
            return ClassFile.of().parse(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Set<String> referencesOf(ClassModel model) {
        Set<String> references = new TreeSet<>();
        for (PoolEntry entry : model.constantPool()) {
            if (entry instanceof ClassEntry classEntry) {
                collectClassNames(classEntry.asSymbol().descriptorString(), references);
            }
        }
        model.fields().forEach(field ->
                collectClassNames(field.fieldTypeSymbol().descriptorString(), references));
        model.methods().forEach(method -> {
            collectClassNames(method.methodTypeSymbol().returnType().descriptorString(), references);
            for (ClassDesc parameter : method.methodTypeSymbol().parameterList()) {
                collectClassNames(parameter.descriptorString(), references);
            }
        });
        return references;
    }

    /** Pulls every "Lcom/foo/Bar;" class name out of a descriptor (handles arrays and methods). */
    private static void collectClassNames(String descriptor, Set<String> into) {
        int start = descriptor.indexOf('L');
        while (start >= 0) {
            int end = descriptor.indexOf(';', start);
            if (end < 0) {
                return;
            }
            into.add(descriptor.substring(start + 1, end).replace('/', '.'));
            start = descriptor.indexOf('L', end);
        }
    }
}
