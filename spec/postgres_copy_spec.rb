require 'spec_helper'

module PG
  class Error
  end
end

describe VoterFile::PostgresCopy do
  let(:raw_conn) { stub(name: "raw_connection") }
  let(:conn) { stub(name: "connection", raw_connection: raw_conn) }

  before do
    raw_conn.stub(:put_copy_data) { |buffer| true }
    raw_conn.stub(:put_copy_end) { |buffer| true }
    raw_conn.stub(:exec) { |sql| }
  end

  it 'should raise an error when copy fails' do
    res = stub('result')
    res.stub(result_error_message: "hello")
    raw_conn.stub(get_result: res)
    expect do
      VoterFile::PostgresCopy.copy('table_name', [:column], conn) do |writer|
        writer.write('hello')
      end
    end.to raise_error VoterFile::PostgresCopy::Error, "hello"
  end
end
