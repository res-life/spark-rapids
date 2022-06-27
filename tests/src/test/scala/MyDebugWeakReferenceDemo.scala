/*
 * Copyright (c) 2019-2022, NVIDIA CORPORATION.
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
 */


import java.lang.ref.{ReferenceQueue, WeakReference}

import com.nvidia.spark.rapids.GpuExpressionTestSuite

class Widget {}

class MySuite extends GpuExpressionTestSuite {

  test("debug 1") {
    val widget = new Widget
    val weakRef = new WeakReference[Widget](widget)
    val w = weakRef.get()
  }

  test("debug 2") {
    val widget = new Widget
    // track the dead obj
    val queue = new ReferenceQueue[Widget]()
    val weakRef = new WeakReference[Widget](widget, queue)
  }

  test("debug 3") {
    // construct a mutable map
    import scala.collection.mutable.{Map => mutableMap}
    val map: mutableMap[String, WeakReference[Widget]] = mutableMap()

    // function to add a week reference
    def addOneIntoMap(map: mutableMap[String, WeakReference[Widget]]): Unit = {
      val widget = new Widget
      val weakWidget = new WeakReference[Widget](widget)
      map("key1") = weakWidget
    }

    addOneIntoMap(map)

    // if GC, the `weak.get()` is null, otherwise is not null
    System.gc()

    // check if reclaimed
    val weak = map("key1")
    val w = weak.get()
    if(w == null) {
      println("reclaimed")
    } else {
      println("not reclaimed")
    }
  }

}
