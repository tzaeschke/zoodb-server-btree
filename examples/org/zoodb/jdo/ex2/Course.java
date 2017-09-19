/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
 * 
 * This file is part of ZooDB.
 * 
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * See the README and COPYING files for further information. 
 */
package org.zoodb.jdo.ex2;

import java.util.HashSet;
import java.util.Set;

import org.zoodb.api.impl.ZooPC;

public class Course extends ZooPC {

	private String name;

	private Set<Student> students = new HashSet<Student>();
	private Teacher teacher;
	
	@SuppressWarnings("unused")
	private Course() {
		//just for ZooDB
	}

	public Course(Teacher teacher, String name) {
		this.name = name;
		this.teacher = teacher;
	}
	
	@Override
	public String toString() {
		zooActivateRead();
		return "Course: " + name;
	}

	public void addStudents(Student ... students) {
		zooActivateWrite();
		for (Student s: students) {
			this.students.add(s);
		}
	}

	public String getName() {
		zooActivateRead();
		return name;
	}

	public Set<Student> getStudents() {
		zooActivateRead();
		return students;
	}
	
	public Teacher getTeacher() {
		zooActivateRead();
		return teacher;
	}
}
