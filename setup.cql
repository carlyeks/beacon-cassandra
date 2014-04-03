-- Copyright (C) 2014. Carl Yeksigian.
--
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation, either version 3 of the License, or
-- (at your option) any later version.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with this program.  If not, see <http://www.gnu.org/licenses/>.

CREATE KEYSPACE IF NOT EXISTS
       beacon
       WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE IF NOT EXISTS 
       beacon.locations (
                        referenceName ascii, 
                        location bigint, 
                        base ascii, 
                        PRIMARY KEY ((referenceName, location, base))
                        )
       WITH comment = 'Reference name and location, along with all bases at that position';

