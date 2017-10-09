require "spec_helper"

RSpec.describe Resque::Fifo::Queue do
  it "has a version number" do
    expect(Resque::Fifo::Queue::VERSION).not_to be nil
  end

  it "does something useful" do
    expect(false).to eq(true)
  end
end
