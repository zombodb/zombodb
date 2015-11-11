/*
 * Portions Copyright 2015 ZomboDB, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */package com.tcdi.zombodb.test;

import java.io.File;

/**
 * Created by e_ridge on 11/11/15.
 */
public class TestingHelper {

    public static void deleteDirectory(File dir) {
        if (dir == null)
            return;

        File[] contents = dir.listFiles();
        for (int x = 0; contents != null && x < contents.length; x++) {
            if (contents[x].isDirectory())
                deleteDirectory(contents[x]);
            else
                contents[x].delete();
        }

        dir.delete();
    }

}
