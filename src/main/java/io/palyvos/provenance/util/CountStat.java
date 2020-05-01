/*  Copyright (C) 2017  Vincenzo Gulisano
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *  Contact: Vincenzo Gulisano info@vincenzogulisano.com
 *
 */

package io.palyvos.provenance.util;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class CountStat {

  private final PrintWriter out;
  private long count;
  private long prevSec;

  public CountStat(String outputFile, boolean autoFlush) {
    this.count = 0;

    FileWriter outFile;
    try {
      outFile = new FileWriter(outputFile);
      out = new PrintWriter(outFile, autoFlush);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }

    prevSec = System.currentTimeMillis() / 1000;

  }

  public void increase(long v) {
    long thisSec = System.currentTimeMillis() / 1000;
    while (prevSec < thisSec) {
      out.println(prevSec + "," + count);
      count = 0;
      prevSec++;
    }
    count += v;
  }

  public void close() {
    long thisSec = System.currentTimeMillis() / 1000;
    while (prevSec <= thisSec) {
      out.println(prevSec + "," + count);
      count = 0;
      prevSec++;
    }
    out.flush();
    out.close();
  }
}
