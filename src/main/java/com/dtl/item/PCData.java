package com.dtl.item;

public class PCData {
	private String houseId;
	private int status;
	
	/**
	 * 状态是否发生了变化
	 */
	private boolean isChanged = false;

	public String getHouseId() {
		return houseId;
	}

	public void setHouseId(String houseId) {
		this.houseId = houseId;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}
	
	public static PCData create() {
		return new PCData();
	}
	
	@Override
	public String toString() {		
		return houseId;
	}

	public boolean isChanged() {
		return isChanged;
	}

	public void setChanged(boolean isChanged) {
		this.isChanged = isChanged;
	}
}
