/* 
This file is part of the PolePosition database benchmark
http://www.polepos.org

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public
License along with this program; if not, write to the Free
Software Foundation, Inc., 59 Temple Place - Suite 330, Boston,
MA  02111-1307, USA. */

package org.zoodb.profiler.test.data;

import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.test.data.CheckSummable;

/**
 * @author Herkules
 */
public class JdoIndexedPilot extends PersistenceCapableImpl implements CheckSummable {
	private		String	mName;
	private		String	mFirstName;
	private		int		mPoints;
	private		int		mLicenseID;

	
	/**
	 * Default.
	 */
	public JdoIndexedPilot()
	{
	}
	
	
	/** 
	 * Creates a new instance of Pilot.
	 */
	public JdoIndexedPilot(String name, int points)
	{
		this.mName=name;
		this.mPoints=points;
	}
	
	/**
	 * Full ctor.
	 */ 
	public JdoIndexedPilot( String name, String frontname, int points, int licenseID )
	{
		mName		= name;
		mFirstName	= frontname;
		mPoints		= points;
		mLicenseID	= licenseID;
	}
	
	
	public int getPoints()
	{
		activateRead("mPoints");
		return mPoints;
	}
	
	public void setPoints( int points )
	{
		activateWrite("mPoints");
		mPoints = points;
	}

	public void addPoints(int points)
	{
		activateWrite("mPoints");
		this.mPoints+=points;
	}
	
	public String getName()
	{
		activateRead("mName");
		return mName;
	}

	public void setName( String name )
	{
		activateWrite("mName");
		mName = name;
	}

	public String getFrontName()
	{
		activateRead("mFirstName");
		return mFirstName;
	}
	
	public void setFrontName( String firstname )
	{
		activateWrite("mFirstName");
		mFirstName = firstname;
	}
	
	public int getLicenseID()
	{
		activateRead("mLicenseID");
		return mLicenseID;
	}
	
	public void setLicenseID( int id )
	{
		activateWrite("mLicenseID");
		mLicenseID = id;
	}

	public String toString()
	{
		activateRead("mName");
		return mName+"/"+mPoints;
	}
    
    public long checkSum() {
        return getPoints();
    }

}
