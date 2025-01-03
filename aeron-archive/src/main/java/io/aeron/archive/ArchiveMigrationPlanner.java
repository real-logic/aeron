/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.archive;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates a series of migration steps given a starting semantic version.
 * <p>
 * Migration steps are stored statically in a list sorted by order of operation. Each has
 * a minimum version. The first step that has a minimum version greater than the passed in version forms
 * the start of the migration steps. All steps afterward are included in the migration.
 * <p>
 * A step need not be a complete operation. A series of operations may be broken down in steps and
 * included with the same minimum version.
 */
final class ArchiveMigrationPlanner
{
    private static final ArrayList<ArchiveMigrationStep> ALL_MIGRATION_STEPS = new ArrayList<>();

    static
    {
        ALL_MIGRATION_STEPS.add(new ArchiveMigration_0_1());
        ALL_MIGRATION_STEPS.add(new ArchiveMigration_1_2());
        ALL_MIGRATION_STEPS.add(new ArchiveMigration_2_3());
        // as migrations are added, they are added to the static list in order of operation
    }

    private ArchiveMigrationPlanner()
    {
    }

    static List<ArchiveMigrationStep> createPlan(final int version)
    {
        final List<ArchiveMigrationStep> steps = new ArrayList<>();

        for (int i = 0, size = ALL_MIGRATION_STEPS.size(); i < size; i++)
        {
            if (ALL_MIGRATION_STEPS.get(i).minimumVersion() > version)
            {
                steps.addAll(ALL_MIGRATION_STEPS.subList(i, size));
                break;
            }
        }

        return steps;
    }
}
