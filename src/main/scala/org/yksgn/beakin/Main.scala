/**
  *     Copyright (C) 2014. Carl Yeksigian.
  * 
  *     This program is free software: you can redistribute it and/or modify
  *     it under the terms of the GNU General Public License as published by
  *     the Free Software Foundation, either version 3 of the License, or
  *     (at your option) any later version.
  * 
  *     This program is distributed in the hope that it will be useful,
  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  *     GNU General Public License for more details.
  * 
  *     You should have received a copy of the GNU General Public License
  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
  */

package org.yksgn.beakin

object Main extends App {
  override def main(args: Array[String]) {
    args(0).toLowerCase match {
      case "import" => Import.exec(args.drop(1))
      case "server" => Server.exec(args.drop(1))
    }
  }
}
